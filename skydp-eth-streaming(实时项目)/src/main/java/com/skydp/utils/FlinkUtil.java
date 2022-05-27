package com.skydp.utils;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.lang.reflect.Field;

public class FlinkUtil {
    /**
     * 初始化flink的执行环境并返回
     * @param jobName:作业名称
     * @param isModifyConfig：是否修改配置
     * @return StreamExecutionEnvironment:flink执行环境
     */
    public static StreamExecutionEnvironment getStreamingEnv(String jobName, boolean isModifyConfig) {
        //Create the Flink flow processing execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (isModifyConfig) {
            //Parallelism is synchronized with the Kafka partition
            //env.setParallelism(3);
            //checkpoint every 5 minutes
            env.setStateBackend(new HashMapStateBackend());
            //set checkpoint's interval
            env.enableCheckpointing(ConfigUtil.getLongConfig("CHECKPOINT_INTERVAL", 10000));
            //checkpoint mode
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            //storage path
            env.getCheckpointConfig().setCheckpointStorage(ConfigUtil.getStringConfig("CHECKPOINT_PATH_CONFIG") + jobName);
            //checkpoint has to be done in one minute or it's thrown out
            env.getCheckpointConfig().setCheckpointTimeout(ConfigUtil.getLongConfig("CHECKPOINT_TIMEOUT", 30000));
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(ConfigUtil.getIntConfig("MAX_CONCURRENT_CHECKPOINTS", 1));
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(ConfigUtil.getIntConfig("MIN_PAUSE_BETWEEN_CHECKPOINTS", 2000));
        }
        return env;
    }


    /**
     *
     * @param env flinstreaming执行的环境
     * @param catalog 设置catalog
     * @return StreamTableEnvironment
     */
    public static StreamTableEnvironment getStreamTableEnv(StreamExecutionEnvironment env, String catalog) {
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
        if (catalog != null) {
            tableEnv.useCatalog("eth");
        }
        return tableEnv;
    }


    /**
     * 类转化成建表 sql语句; 类的成员变量只能为基本类型或者string
     * @param clazz 类class (not null)
     * @param tableName  表名(not null)
     * @param waterMakerColumnName 哪个列名确定为wartermark时间(not null)
     * @param interval watermark延迟时间（>= 0s）
     * @return string example: create table name (.....)
     */
    public static String classToCreateKafkaTableSql(Class<?> clazz, String tableName,
                                                    String waterMakerColumnName, int interval,
                                                    String topic, String groupId) {

        //限制参数
        assert clazz != null;
        assert tableName !=null;
        assert waterMakerColumnName != null;
        assert  interval > 0;
        assert topic != null;
        assert groupId != null;

        String createTableSql;
        //生成sql语句
        String sqlTmp = "create table %s ( %s WATERMARK FOR %s AS %s - INTERVAL '%d' SECOND) %s";
        //获取列名和类型
        StringBuilder columnNameType = new StringBuilder();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            String type = field.getType().getSimpleName();
            //判断该列是否为水位时间
            if (name.equalsIgnoreCase(waterMakerColumnName)){
                //变量为long 则列类型为timestamp_ltz(3), 其他类型为timestamp(3)
                if ("Long".equalsIgnoreCase(type)) {
                    type = "TIMESTAMP_LTZ(3)";
                } else {
                    type = "TIMESTAMP(3)";
                }

            } else if ("Long".equalsIgnoreCase(type)) { //long转为bigint
                type = "Bigint";
            }
            columnNameType.append(name).append(" ").append(type).append(",").append(" ");
        }

        //kafka配置
        String kafkaConfig = String.format("with (" +
                "    'connector' = 'kafka', " +
                "    'properties.bootstrap.servers' = '%s'," +
                "    'topic' = '%s', " +
                "    'properties.group.id' = '%s'," +
                "    'scan.startup.mode' = 'latest-offset'," +
                "    'format' = 'json'," +
                "    'value.json.fail-on-missing-field' = 'true'," +
                "    'json.ignore-parse-errors' = 'false')",
                ConfigUtil.getStringConfig("BOOTSTRAP_SERVERS"), topic, groupId);

        createTableSql = String.format(sqlTmp, tableName, columnNameType, waterMakerColumnName, waterMakerColumnName, interval, kafkaConfig);

        return createTableSql;
    }

}
