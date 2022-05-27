package com.skydp.dws.nfttrades;

import com.skydp.bean.dwd.BlockTxn;
import com.skydp.bean.dwd.LogErc;
import com.skydp.bean.dws.log.OpenSea;
import com.skydp.utils.ClickhouseUtil;
import com.skydp.utils.FlinkUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import java.util.ArrayList;

public class ParseNFTTradesJob {
    private static final String JOB_NAME = "dws_parse_nft_trades";
    private static final String KAFKA_DWD_BLOCK_TXN_TOPIC = "dwd_eth_block_txn";
    private static final String KAFKA_DWD_LOG_ERC_TOPIC = "dwd_eth_log_erc";
    private static final String KAFKA_GROUPID = "dws_parse_nft_trades_groupid_01";
    private static final String CLICKHOUSE_TABLE = "dws_nft_trades_v1_realtime";

    public static void main(String[] args) throws Exception {
        //fink streaming环境
        StreamExecutionEnvironment env = FlinkUtil.getStreamingEnv(JOB_NAME, true);

        //flink streaming table环境
        StreamTableEnvironment tableEnv = FlinkUtil.getStreamTableEnv(env, "eth");

        //生成dwd_eth_block_txn的建表语句，并建立相关表
        String blockTxnTable = FlinkUtil.classToCreateKafkaTableSql(
                BlockTxn.class, KAFKA_DWD_BLOCK_TXN_TOPIC, "blockTime",
                30, KAFKA_DWD_BLOCK_TXN_TOPIC, KAFKA_GROUPID);

        tableEnv.executeSql(blockTxnTable);

        //生成dwd_eth_block_txn的建表语句，并建立相关表
        String logErcTable = FlinkUtil.classToCreateKafkaTableSql(
                LogErc.class, KAFKA_DWD_LOG_ERC_TOPIC, "blockTime",
                30, KAFKA_DWD_LOG_ERC_TOPIC, KAFKA_GROUPID);

        tableEnv.executeSql(logErcTable);

        //建立事务临时表
        Table txnTmptable = tableEnv.sqlQuery(
                " select  txnHash, " +
                        " window_start, " +
                        " window_end, " +
                        " concat_ws('#', cast(blockHeight as string), cast(blockTime as string), txnHash, " +
                                   " txnFrom, txnTo, cast(txnChainId as string), txnInputData) as txnLine " +
                " from table ( tumble(table dwd_eth_block_txn, descriptor(blockTime), interval '15' seconds)) " +
                " where txnInputData is not null and  txnInputData != '' and txnStatus != 0");

        tableEnv.createTemporaryView("txn_tmp", txnTmptable);

        //建立日志临时表
        Table logTmpTable = tableEnv.sqlQuery(
                " select txnHash, " +
                       " window_start, " +
                       " window_end, " +
                       " concat_ws('_', " +
                               " collect(concat_ws('=', address, topics, data, ercType, " +
                                                 " cast(decimals as string), " +
                                                 " cast(logIndex as string)))) as logLine " +
                " from table ( tumble(table dwd_eth_log_erc, descriptor(blockTime), interval '15' seconds)) " +
                " group by window_start, window_end, txnHash, blockTime");

        tableEnv.createTemporaryView("log_tmp", logTmpTable);


        //窗口join txn和log表
        Table txnJoinLogTable = tableEnv.sqlQuery(
                " select concat_ws('#', txnLine, logLine)" +
                        " from txn_tmp tt join log_tmp lt on tt.txnHash = lt.txnHash and " +
                        "tt.window_start = lt.window_start and tt.window_end = lt.window_end ");

        //转化成算子(append_only)
        DataStream<String> txnLogStrDS = tableEnv.toDataStream(txnJoinLogTable, String.class);

        //解析opensea 很复杂，可以不看
        SingleOutputStreamOperator<OpenSea> openseaFlatMap = txnLogStrDS.flatMap(new ParseOpenseaFlatMap());

        //数据下沉
        String ckInsertSql = ClickhouseUtil.classToInsertSql(OpenSea.class, CLICKHOUSE_TABLE);
        SinkFunction clickhouseSink = ClickhouseUtil.getJdbcSink(ckInsertSql);
        openseaFlatMap.addSink(clickhouseSink);

        env.execute(JOB_NAME);
    }

    /**
     * 将数据解析出opensea
     */
    static class ParseOpenseaFlatMap implements FlatMapFunction<String, OpenSea> {
        @Override
        public void flatMap(String value, Collector<OpenSea> out) throws Exception {
            //获得opensead的解析类
            ParseNFTTrades parseNFTTrades = new ParseNFTTrades();
            //进行解析
            ArrayList<OpenSea> openSeas = parseNFTTrades.parseStrToOpenseaList(value);
            //打印输出
            for (OpenSea openSea : openSeas) {
                out.collect(openSea);
            }
        }
    }
}
