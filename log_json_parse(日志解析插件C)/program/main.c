#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <dirent.h>
#include <time.h>
#include <stdbool.h>
#include "cJson.h"

#define MAX_FILE_NUM  1000

char *get_file_json(const char *filename) {
    FILE *f;long len;char *data;

    //打开文件
    f = fopen(filename, "rb");
    if (f == NULL) {
        fprintf(stderr, "[get_file_json line:%d][ERROR]Cannot open the file:%s ...\n", __LINE__ ,filename);
                exit(1);
    }

    //获取最后指针
    fseek(f, 0, SEEK_END);
    len = ftell(f);

    //读取文件数据
    fseek(f, 0, SEEK_SET);
    data = (char*)malloc(len + 1);
    fread(data, 1, len, f);

    //关闭数据
    fclose(f);
    return data;
}

char *get_file_name(const char *des_dir) {
    char *file_name;

    //char类型的时间戳
    char *time_str = (char *)malloc(sizeof(long));

    //获取时间
    long now_time;
    now_time = (long)time(NULL);
    //ltoa(now_time, time_str, 10);

    sprintf(time_str,"%ld",now_time);

    //创建文件名称
    file_name = (char *) malloc (strlen(des_dir) + strlen(time_str) + strlen(".json") + 1);
    strcpy(file_name, des_dir);
    strcat(file_name, time_str);
    strcat(file_name, ".json");

    free(time_str);
    return file_name;
}


cJSON *parse_json_array(char *text) {
    cJSON *json;

    json = cJSON_Parse(text);
    if (!json) {
        fprintf(stderr ,"[parse_json_array Line: %d][ERROR]: [%s]\n", __LINE__ ,cJSON_GetErrorPtr());
        return NULL;
    }
//    size = cJSON_GetArraySize(json);
    return json;
}

void parse_directory(const char *src_dir, const char *des_dir, const char *start_file_name) {
    //一次读取文件最大数目
    char *file_name[MAX_FILE_NUM];

    DIR *dir = NULL;
    //打开目录
    dir = opendir(src_dir);
    if (!dir) {
        fprintf(stderr, "[parse_directory Line: %d][ERROR]opendir: %s\n",__LINE__ ,strerror(errno));
        return;
    }

    int count = 0;
    //读取文件夹下面的文件
    bool is_start = false;
    struct dirent *dirp = NULL;
    while((dirp = readdir(dir))) {
        if (strcmp(dirp->d_name, ".") == 0 || strcmp(dirp->d_name, "..") == 0 ) {
            continue;
        }

        //判断文件是否已经读取(已经有文件被读取, 且现在在读以前文件)
        if(start_file_name != NULL && !is_start) {
            //判断是否为最后一个文件
            if(strcmp(dirp->d_name, start_file_name) == 0) {
                is_start = true;
            }
            continue;
        }

        //获取指定目录下的文件
        file_name[count] = (char *) malloc (strlen(src_dir)+strlen(dirp->d_name)+1);
        strcpy(file_name[count], src_dir);
        strcat(file_name[count], dirp->d_name);

        //文件数目加1
        count++;
        if(count >= MAX_FILE_NUM) {
            //获取目的文件名
            char *des_file_name = get_file_name(des_dir);
            //创建目的文件
            FILE *des_file = fopen(des_file_name, "wb");
            if (des_file == NULL) {
                 fprintf(stderr ,"[parse_directory Line: %d][ERROR]Cannot open the destination file: %s...\n",__LINE__ ,des_file_name);
                         exit(1);
            }

            for(int i = 0; i < count; i++) {
              fprintf(stdout, "[parse_directory Line: %d][Stdout]: start wirte, filename: %s\n",__LINE__ ,file_name[i]);
              char *data = get_file_json(file_name[i]);

              //对json进行解析
              cJSON *json_arr = parse_json_array(data);

              if(json_arr == NULL) {
                 //释放file_name的空间
                 fprintf(stdout, "[parse_directory Line: %d][ERROR]: parse json fail, filename: %s\n",__LINE__ ,file_name[i]);
                 free(data);
                 free(file_name[i]);
                 continue;
              }

              //获取json长度
              int size = cJSON_GetArraySize(json_arr);
              //对json数组进行解析
              for(int j = 0; j < size; j++) {
                cJSON *line_json = cJSON_GetArrayItem(json_arr, j);
                char *line = cJSON_PrintUnformatted(line_json);
                //写入相关文件
                fprintf(des_file, "%s\n", line);
                //释放内存空间
                free(line);
              }
              fprintf(stdout, "[parse_directory Line: %d][Stdout]: wirte success, filename: %s\n",__LINE__ ,file_name[i]);
              //清除json_arr
              cJSON_Delete(json_arr);
              //释放file_name的空间
              free(data);
              free(file_name[i]);
            }
            //释放文件名内存
            free(des_file_name);
            //关闭目标文件
            fclose(des_file);
            //初始化文件数目
            count = 0;
        }
    }
    //最后文件写入一个文件中
    if(count > 0) {
       //获取目的文件名
       char *des_file_name = get_file_name(des_dir);
       //创建目的文件
       FILE *des_file = fopen(des_file_name, "wb");
       if (des_file == NULL) {
           fprintf(stderr ,"[parse_directory Line:%d][ERROR]Cannot open the destination file: %s...\n",__LINE__ ,des_file_name);
           exit(1);
       }
       for(int i = 0; i < count; i++) {
           fprintf(stdout, "[parse_directory Line: %d][Stdout]: start wirte, filename: %s\n",__LINE__ ,file_name[i]);
           char *data = get_file_json(file_name[i]);
           //对json进行解析
           cJSON *json_arr = parse_json_array(data);
            if(json_arr == NULL) {
                 //释放file_name的空间
                 fprintf(stdout, "[parse_directory Line: %d][ERROR]: parse json fail, filename: %s\n",__LINE__ ,file_name[i]);
                 free(data);
                 free(file_name[i]);
                 continue;
              }


           //获取json长度
           int size = cJSON_GetArraySize(json_arr);
           //对json数组进行解析
           for(int j = 0; j < size; j++) {
               cJSON *line_json = cJSON_GetArrayItem(json_arr, j);
               char *line = cJSON_PrintUnformatted(line_json);
               //写入相关文件
               fprintf(des_file, "%s\r\n", line);
               free(line);
           }
           fprintf(stdout, "[parse_directory  Line: %d][Stdout]: wirte success, filename: %s\n",__LINE__ ,file_name[i]);
           //清除json_arr
           cJSON_Delete(json_arr);
           //释放file_name的空间
           free(data);
           free(file_name[i]);
        }
        //释放文件名内存
        free(des_file_name);
        //关闭目标文件
        fclose(des_file);
        //初始化文件数目
        count = 0;
    }
    closedir(dir);
}

int main (int argc, const char * argv[]) {
        if (argc < 3) {
        fprintf(stderr, "[main Line: %d][ERROR]Usage: too few paramters!\n", __LINE__);
        return 0;
        }

        //解析文件夹的json字符串
        //parse_directory("G:/codeblocks/src/","G:/codeblocks/des/", "logs02.json");
        if (argc == 3) {
        parse_directory(argv[1], argv[2], NULL);
        } else {
            parse_directory(argv[1], argv[2], argv[3]);
        }

        return 0;
}
