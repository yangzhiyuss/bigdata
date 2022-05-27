package com.skydp.utils;


import com.skydp.bean.TransientField;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.lang.reflect.Field;

/**
 * clickSink的工具类
 */
public class ClickhouseUtil {
    /**
     * @param sql:输入要执行的sql语句
     * @param <T>：数据的类型
     * @return：SinkFunction 获取ClickHouseSink
     */
    public static <T> SinkFunction getJdbcSink(String sql) {
        SinkFunction<T> sink = JdbcSink.sink(
                sql,
                (jdbcPreparedStatement, base) -> {
                    //通过反射获取相关类的成员变量
                    Field[] fields = base.getClass().getDeclaredFields();
                    int skipOffset = 0; //
                    for (int i = 0; i < fields.length; i++) {
                        Field field = fields[i];
                        //通过反射获得字段上的注解
                        TransientField transientFiled =
                                field.getAnnotation(TransientField.class);
                        if (transientFiled != null) {
                            // 如果存在该注解
                            skipOffset++;
                            continue;
                        }
                        field.setAccessible(true);
                        try {
                            Object o = field.get(base);
                            jdbcPreparedStatement.setObject(i + 1 - skipOffset, o);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(5000).withBatchIntervalMs(200).withMaxRetries(5).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(ConfigUtil.getStringConfig("CLICKHOUSE_URL"))
                        .withDriverName(ConfigUtil.getStringConfig("Driver_NAME"))
                        .withUsername(ConfigUtil.getStringConfig("CK_USER_NAME"))
                        .build());
        return sink;
    }


    /**
     *
     * @param clazz 类名 注：类的成员只能时基本变量，不能是组合类
     * @param tableName clickhouse表面
     * @return
     */
    public static String classToInsertSql(Class<?> clazz, String tableName) {
        assert clazz != null;
        assert tableName != null;

        String insertSql;
        String tmpSql = "insert into %s values( %s )";
        StringBuilder paramStrBuilder = new StringBuilder();
        Field[] fields = clazz.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            paramStrBuilder.append("?").append(", ");
        }
        String substr = paramStrBuilder.substring(0, paramStrBuilder.length() - 2);
        insertSql = String.format(tmpSql, tableName, substr);
        return insertSql;
    }
}
