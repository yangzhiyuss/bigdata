package com.skydp.dwd.logerc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skydp.bean.dwd.LogErc;
import com.skydp.utils.FlinkUtil;
import com.skydp.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class ParseLogErcJob {
    private static final String JOB_NAME = "dwd_eth_log_erc";
    private static final String KAFKA_ODS_TOPIC = "erc_token_realtime_data";
    private static final String KAFKA_DWD_TOPIC = "dwd_eth_log_erc";
    private static final String KAFKA_GROUPID = "dwd_eth_log_erc_groupid_01";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtil.getStreamingEnv(JOB_NAME, true);

        System.setProperty("HADOOP_USER_NAME","hdfs");

        //获取kafkasource,写入数据
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(KAFKA_ODS_TOPIC, KAFKA_GROUPID);
        DataStreamSource<String> logErcSource = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
//                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(1))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
//                            @Override
//                            public long extractTimestamp(String element, long recordTimestamp) {
//                                return JSON.parseObject(element).getLong("timestamp");
//                            }
//                        }),
                "dwd_logerc_source");

//        DataStreamSource<String> logErcSource = env.readTextFile("D:\\idea\\workspace\\skydp-eth-streaming\\src\\main\\resources\\log_erc01.json");

        //拆分json数组
        SingleOutputStreamOperator<JSONObject> logErcJsonFlatMap = logErcSource.flatMap(new LogErcJsonFlatMap());

        //解析logErc
        SingleOutputStreamOperator<LogErc> logErcFlatMap = logErcJsonFlatMap
                .keyBy(data -> data.getString("transactionHash") + data.getString("logIndex"))
                .flatMap(new LogErcFlatMap());

        //转化成jsonStrr
        SingleOutputStreamOperator<String> logErcJsonStrMap = logErcFlatMap.map(data -> JSON.toJSON(data).toString());

//        logErcJsonStrMap.print();

        //建立sink
        KafkaSink<String> kafkaSink = KafkaUtil.getKafkaSink(KAFKA_DWD_TOPIC);
        logErcJsonStrMap.sinkTo(kafkaSink);

        env.execute(JOB_NAME);
    }


    /**
     * 解析logerc,并去重复
     */
    static class LogErcFlatMap extends RichFlatMapFunction<JSONObject, LogErc> {
        private transient ValueState<String> logId;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>(
                            "logId", // the state name
                            String.class);

            //设置状态值的TTL时间为一分钟
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.minutes(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            descriptor.enableTimeToLive(ttlConfig);

            logId = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(JSONObject value, Collector<LogErc> out) throws Exception {
            if (value == null) {
                return;
            }

            String txnHashLogIndex = value.getString("transactionHash") + value.getString("logIndex");
            if (logId.value() != null && logId.value().equals(txnHashLogIndex)) {
                return;
            }
            //updata the status
            logId.update(txnHashLogIndex);

            //解析json
            ArrayList<LogErc> logErcs = LogErc.parseJsonToLogErcList(value);
            for (LogErc logErc : logErcs) {
                out.collect(logErc);
            }

        }
    }


    /**
     * 解析出来json数组
     */
    static class LogErcJsonFlatMap implements FlatMapFunction<String, JSONObject> {
        @Override
        public void flatMap(String value, Collector<JSONObject> out) throws Exception {
            JSONArray jsonArr = JSON.parseArray(value);
            //判断解析的jsonarr是否空，若为空返回
            if (jsonArr == null || jsonArr.size() == 0) {
                return;
            }

            for (int i = 0; i < jsonArr.size(); i++) {
                //过滤空值
                if (jsonArr.getJSONObject(i) != null) {
                    out.collect(jsonArr.getJSONObject(i));
                }
            }
        }
    }

}
