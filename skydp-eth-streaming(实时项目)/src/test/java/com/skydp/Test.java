package com.skydp;

import com.skydp.bean.dwd.BlockTxn;
import com.skydp.bean.dws.log.OpenSea;
import com.skydp.utils.ClickhouseUtil;
import com.skydp.utils.ConfigUtil;
import com.skydp.utils.FlinkUtil;
import org.apache.flink.streaming.api.CheckpointingMode;

public class Test {
    public static void main(String[] args) {
//        System.out.println(ConfigUtil.getLongConfig("CHECKPOINT_INTERVAL", 10000));
//        //checkpoint mode
//        System.out.println(CheckpointingMode.EXACTLY_ONCE);
//        //storage path
//        System.out.println(ConfigUtil.getStringConfig("CHECKPOINT_PATH_CONFIG"));
//        //checkpoint has to be done in one minute or it's thrown out
//        System.out.println(ConfigUtil.getLongConfig("CHECKPOINT_TIMEOUT", 30000));
//        System.out.println(ConfigUtil.getIntConfig("MAX_CONCURRENT_CHECKPOINTS", 1));
//        System.out.println(ConfigUtil.getIntConfig("MIN_PAUSE_BETWEEN_CHECKPOINTS", 2000));
//
//        String sql = FlinkUtil.classToCreateKafkaTableSql(
//                BlockTxn.class, "dwd_block_txn", "blockTime",
//                30, "dwd_block_txn", "dwd_block_txn_groupId_01");
//        System.out.println(sql);

        String ck_table = ClickhouseUtil.classToInsertSql(OpenSea.class, "ck_table");
        System.out.println(ck_table);

    }
}
