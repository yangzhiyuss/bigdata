package com.skydp.bean.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

@Data
@AllArgsConstructor
public class BlockTxn {
    //'块高度',
    private long blockHeight;
    //'块hash',
    private String blockHash;
//    parent_hash string COMMENT '父块hash',
    private String parentHash;
//    sha3_uncles string COMMENT '叔块hash',
    private String sha3Uncles;
//    miner string COMMENT 'miner地址',
    private String miner;
//    state_root string COMMENT '状态root',
    private String stateRoot;
//    transactions_root string COMMENT '事务root',
    private String transactionRoot;
//    receipts_root string COMMENT '收据root',
    private String receiptsRoot;
//    difficulty string COMMENT '块难度',
    private String difficulty;
//    block_gaslimit decimal(30,0) COMMENT 'gas的最大限度',
    private String blockGasLimit;
//    block_gasused decimal(30, 0) COMMENT '块已使用gas',
    private String blockGasused;
//    block_basefee string COMMENT '块燃烧',
    private String blockBaseFee;
//    extradata string COMMENT '额外数据',
    private String extraData;
//    mixHash string COMMENT '最大hash',
    private String mixHash;
//    block_nonce string COMMENT '块nonce',
    private String blockNonce;
//    block_unclesnumber int COMMENT '叔块数目',
    private int blockUnclesNumber;
//    block_size bigint COMMENT '块大小',
    private long blockSize;
//    received_from string,
    private String receivedFrom;
//    txn_hash string COMMENT '交易hash',
    private String txnHash;
//    txn_from string COMMENT '交易发送地址',
    private String txnFrom;
//    txn_to string COMMENT '交易接收地址',
    private String txnTo;
//    txn_contractaddress string COMMENT '合约创建地址',
    private String txnContractAddress;
//    txn_size bigint COMMENT '交易大小',
    private long txnSize;
//    txn_type int COMMENT '交易类型',
    private int txnType;
//    txn_status int COMMENT '交易状态，成功为1，失败为0',
    private int txnStatus;
//    txn_chainId int COMMENT '交易链id',
    private int txnChainId;
//    txn_inputdata string COMMENT '交易数据',
    private String txnInputData;
//    txn_value string COMMENT '交易价值',
    private String txnValue;
//    txn_nonce decimal(30, 0) COMMENT '交易nonce',
    private String txnNonce;
//    txn_poststate string COMMENT '保存了该 Receipt对象被创建之后的 StateDB 的 MPT 树根',
    private String txnPostState;
//    txn_gaslimit decimal(30, 0) COMMENT '事务的最大gas限制',
    private String txnGasLimit;
//    txn_gasprice string COMMENT 'gas价格',
    private String txnGasPrice;
//    txn_gastipcap string,
    private String txnGasTipcap;
//    txn_gasfee string COMMENT '每个gas的燃烧价格',
    private String txnGasFee;
//    txn_maxfee string COMMENT '每个gas的最大燃烧价格',
    private String txnMaxFee;
//    txn_maxpriority string COMMENT 'gas燃烧',
    private String txnMaxPriority;
//    txn_gasused decimal(30, 0) COMMENT '本次交易所用的gas量',
    private String txnGasUsed;
//    txn_cumulativegasused decimal(30, 0) COMMENT '累计使用的gas量',
    private String txnCumulativeGasUsed;
//    txn_position bigint COMMENT '交易索引',
    private long txnPosition;
//            `timestamp` string COMMENT '时间戳'
    private String blockTime;


    /**
     * 解析json数组,并将值写入BlockTxn类中
     * @param json
     * @return ArrayList<BlockTxn>
     */
    public static ArrayList<BlockTxn> parseJsonToBlockTxnList(JSONObject json) {
        ArrayList<BlockTxn> blockTxnList = new ArrayList<>();
        //解析json
        long blockHeight = json.getLong("blockHeiget") != null ? json.getLong("blockHeiget"): 0;
        String blockHash = json.getString("blockHash");
        String parentHash = json.getString("parentHash");
        String sha3Uncles = json.getString("sha3Uncles");
        String miner = json.getString("miner");
        String stateRoot = json.getString("stateRoot");
        String transactionsRoot = json.getString("transactionsRoot");
        String receiptsRoot = json.getString("receiptsRoot");
        String difficulty = json.getString("difficulty");
        String blockGasLimit = json.getString("gasLimit");
        String blockGasUsed = json.getString("BlockGasUsed");
        String blockBaseFee = json.getString("blockBaseFee");
        String extraData = json.getString("extraData");
        String mixHash = json.getString("mixDisgest");
        String blockNonce = json.getString("blockNonce");
        int blockUnclesNumber = json.getInteger("unclesNumber") != null ? json.getInteger("unclesNumber"): 0;
        long blockSize = json.getLong("blockSize") != null ? json.getLong("blockSize"): 0;
        String receivedFrom = json.getString("receivedFrom");
        //进行时间解析
        long timeStamp = json.getLong("timeStamp") != null ? json.getLong("timeStamp"): 0;
        String blockTime = LocalDateTime.ofEpochSecond(timeStamp, 0, ZoneOffset.ofHours(8)).toString().replace('T', ' ');

        String transactions = json.getString("Transactions");
        //获取事务数组
        JSONArray txnsJsonArr = JSON.parseArray(transactions);
        //transaction is null, return the result
        if (txnsJsonArr == null || txnsJsonArr.size() == 0) {
            BlockTxn blockTxn = new BlockTxn(blockHeight, blockHash, parentHash, sha3Uncles, miner, stateRoot,
                    transactionsRoot, receiptsRoot, difficulty, blockGasLimit, blockGasUsed, blockBaseFee,
                    extraData, mixHash, blockNonce, blockUnclesNumber, blockSize, receivedFrom, null,
                    null, null, null, 0, 0, 0,
                    0, null, null, null, null,
                    null, null, null, null, null,
                    null, null, null, 0, blockTime);

            blockTxnList.add(blockTxn);
        } else {
            for (int i = 0; i < txnsJsonArr.size(); i++) {
                //化成json
                JSONObject txnJson = txnsJsonArr.getJSONObject(i);
                //解析json
                String txHash = txnJson.getString("txHash");
                String txnFrom = txnJson.getString("TxnFrom");
                String txnTo = txnJson.getString("txnTo");
                String txnContractAddress = txnJson.getString("txnContractAddress");
                long txnSize = txnJson.getLong("size") != null ? txnJson.getLong("size"): 0;
                int txnType = txnJson.getInteger("txnType") != null ? txnJson.getInteger("txnType"): 0;
                int txnStatus = txnJson.getInteger("txnStatus") != null ? txnJson.getInteger("txnStatus") : 0;
                int txnChainId = txnJson.getInteger("chainId") != null ? txnJson.getInteger("chainId") : 0;
                String txnInputData = txnJson.getString("txnInputData");
                String txnValue = txnJson.getString("txnValue");
                String txnNonce = txnJson.getString("txnNonce");
                String txnPostState = txnJson.getString("txnPostState");
                String txnGasLimt = txnJson.getString("txnGasLimt");
                String txnGasPrice = txnJson.getString("txnGasPrice");
                String txnGasTipCap = txnJson.getString("txnGasTipCap");
                String txnGasFee = txnJson.getString("txnGasFeeCap");
                String txnMaxFee = txnJson.getString("txnMaxFee");
                String txnMaxPriority = txnJson.getString("txnMaxPriority");
                String txGasUsed = txnJson.getString("txGasUsed");
                String txnCumulativeGasUsed = txnJson.getString("txnCumulativeGasUsed");
                long txnPosition = txnJson.getLong("txnTransactionIndex") != null ? txnJson.getLong("txnTransactionIndex"): 0;

                BlockTxn blockTxn = new BlockTxn(blockHeight, blockHash, parentHash, sha3Uncles, miner, stateRoot,
                        transactionsRoot, receiptsRoot, difficulty, blockGasLimit, blockGasUsed, blockBaseFee,
                        extraData, mixHash, blockNonce, blockUnclesNumber, blockSize, receivedFrom, txHash, txnFrom,
                        txnTo, txnContractAddress, txnSize, txnType, txnStatus, txnChainId, txnInputData, txnValue,
                        txnNonce, txnPostState, txnGasLimt, txnGasPrice, txnGasTipCap, txnGasFee, txnMaxFee, txnMaxPriority,
                        txGasUsed, txnCumulativeGasUsed, txnPosition, blockTime);

                blockTxnList.add(blockTxn);
            }
        }

        return blockTxnList;
    }

}
