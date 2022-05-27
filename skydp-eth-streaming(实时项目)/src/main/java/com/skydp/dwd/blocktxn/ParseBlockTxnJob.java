package com.skydp.dwd.blocktxn;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skydp.bean.dwd.BlockTxn;
import com.skydp.utils.FlinkUtil;
import com.skydp.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import java.util.Objects;

public class ParseBlockTxnJob {
    private static final String JOB_NAME = "dwd_eth_block_txn";
    private static final String KAFKA_ODS_TOPIC = "eth_block_realtime_data";
    private static final String KAFKA_DWD_TOPIC = "dwd_eth_block_txn";
    private static final String KAFKA_GROUPID = "dwd_eth_block_txn_groupid_01";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtil.getStreamingEnv(JOB_NAME, true);

        System.setProperty("HADOOP_USER_NAME","hdfs");

        //获取kafkasource,写入数据
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(KAFKA_ODS_TOPIC, KAFKA_GROUPID);
        DataStreamSource<String> blocktxnSource = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
//                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(1))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
//                            @Override
//                            public long extractTimestamp(String element, long recordTimestamp) {
//                                return JSON.parseObject(element).getLong("timestamp");
//                            }
//                        }),
                "dwd_blocktxn_source");

//        DataStreamSource<String> blocktxnSource = env.readTextFile("D:\\idea\\workspace\\skydp-eth-streaming\\src\\main\\resources\\block_txn01.json");

        //解析并过滤block_txn
        SingleOutputStreamOperator<BlockTxn> blockTxnFlatMap = blocktxnSource
                .keyBy(data -> JSON.parseObject(data).getString("blockHeiget"))
                .flatMap(new ParseBlockTxnFlatMap());

        //转化成jsonString
        SingleOutputStreamOperator<String> blockTxnJsonStrMap = blockTxnFlatMap.map(data -> JSON.toJSON(data).toString());

//        blockTxnJsonStrMap.print();

        //写入kafkasink
        KafkaSink<String> kafkaSink = KafkaUtil.getKafkaSink(KAFKA_DWD_TOPIC);
        blockTxnJsonStrMap.sinkTo(kafkaSink);

        env.execute(JOB_NAME);
    }


    /**
     * 进行blockTxn解析，一对多,去重
     */
    static class ParseBlockTxnFlatMap extends RichFlatMapFunction<String, BlockTxn> {
        private transient ValueState<Long> blockId;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>(
                            "blockId", // the state name
                            Long.class);

            //设置状态值的TTL时间为一分钟
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.minutes(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            descriptor.enableTimeToLive(ttlConfig);

            blockId = getRuntimeContext().getState(descriptor);
        }


        @Override
        public void flatMap(String value, Collector<BlockTxn> out) throws Exception {
            JSONObject jsonObject = JSON.parseObject(value);

            //判断json是否为空
            if (jsonObject == null) {
                return;
            }
            //获取blockheight 判断是否已经存在, 存在直接返回
            Long blockHeight;
            try {
                blockHeight = jsonObject.getLong("blockHeiget");
                //blockheight is not null
                if (blockHeight == null){
                    return;
                }
            } catch (Exception e) {
                return;
            }
            //if status is not null and equal blockheight,the block already exits;
            if (blockId.value() != null && Objects.equals(blockId.value(), blockHeight)) {
                return;
            }
            //updata the status
            blockId.update(blockHeight);

            //解析json
            ArrayList<BlockTxn> blockTxnList = BlockTxn.parseJsonToBlockTxnList(jsonObject);
            for (BlockTxn blockTxn : blockTxnList) {
                out.collect(blockTxn);
            }
        }
    }
}
