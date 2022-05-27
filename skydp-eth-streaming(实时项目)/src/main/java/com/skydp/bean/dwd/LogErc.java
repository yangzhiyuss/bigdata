package com.skydp.bean.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

@Data
@AllArgsConstructor
public class LogErc {
//    address string COMMENT '合约地址',
    private String address;
//    topics string COMMENT '主题',
    private String topics;
//    `data` string COMMENT '数据',
    private String data;
//    txn_hash string COMMENT '交易hash',
    private String txnHash;
//    txn_index bigint COMMENT 'hash索引',
    private long txnIndex;
//    block_hash string COMMENT '块hash',
    private String blockHash;
//    block_number bigint COMMENT '块号',
    private long blockNumber;
//    log_index bigint COMMENT '日志索引',
    private long logIndex;
//    removed boolean COMMENT '是否是链重组',
    private boolean removed;
//    creator string COMMENT '合约创建地址',
    private String creator;
//    erc_type string COMMENT '代币类型, erc20或者erc721',
    private String ercType;
//    `from` string COMMENT '发送人',
    private String from;
//    `to` string COMMENT '接收人',
    private String to;
//    value string COMMENT '代币金额',
    private String value;
//    from_balance string COMMENT 'from余额',
    private String fromBalance;
//    to_balance string COMMENT 'to余额',
    private String toBalance;
//    address_balance  string COMMENT '地址余额',
    private String addressBalance;
//    name string COMMENT '代币全名称',
    private String name;
//    symbol string COMMENT '代币缩写',
    private String symbol;
//    decimals bigint COMMENT '汇率',
    private long decimals;
//    `timestamp` string COMMENT '时间戳'
    private String blockTime;


    /**
     * 解析json数组,并将值写入LogErc类中
     * @param json
     * @return ArrayList<LogErc>
     */
    public static ArrayList<LogErc> parseJsonToLogErcList(JSONObject json) {
        ArrayList<LogErc> logErcList = new ArrayList<>();
        //解析json
        String address = json.getString("address");
        String topics = json.getString("topics");
        String data = json.getString("data");
        String txnHash = json.getString("transactionHash");
        long txnIndex = json.getLong("transactionIndex") != null ? json.getLong("transactionIndex"): 0;
        String blockHash = json.getString("blockHash");
        long blockNumber = json.getLong("blockNumber") != null ? json.getLong("blockNumber"): 0;
        long logIndex = json.getLong("logIndex") != null ? json.getLong("logIndex"): 0;
        boolean removed = json.getBoolean("removed") != null ? json.getBoolean("removed") : false;
        //进行时间解析
        long timeStamp = json.getLong("timeStamp") != null ? json.getLong("timeStamp"): 0;
        String blockTime = LocalDateTime.ofEpochSecond(timeStamp, 0, ZoneOffset.ofHours(8)).toString().replace('T', ' ');

        //获取erc信息
        String erctokens = json.getString("erctokens");
        JSONArray ercJsonArr = JSON.parseArray(erctokens);
        //erc is null, return
        if (ercJsonArr == null || ercJsonArr.size() == 0) {
            LogErc logErc = new LogErc(address, topics, data, txnHash, txnIndex, blockHash,
                    blockNumber, logIndex, removed, null, null, null,
                    null, null, null, null, null,
                    null, null, 0, blockTime);

            logErcList.add(logErc);
        } else {
            for (int i = 0; i < ercJsonArr.size(); i++) {
                //化成json
                JSONObject ercJson = ercJsonArr.getJSONObject(i);
                //解析json
                String creator = ercJson.getString("creator");
                String ercType = ercJson.getString("ercType");
                String from = ercJson.getString("from");
                String to = ercJson.getString("to");
                String value = ercJson.getString("value");
                String fromBalance = ercJson.getString("fromBalance");
                String toBalance = ercJson.getString("toBalance");
                String addressBalance = ercJson.getString("addressBalance");
                String name = ercJson.getString("name");
                String symbol = ercJson.getString("symbol");
                long decimals = ercJson.getLong("decimals") != null ? ercJson.getLong("decimals"): 0;

                LogErc logErc = new LogErc(address, topics, data, txnHash, txnIndex, blockHash,
                        blockNumber, logIndex, removed, creator, ercType, from, to, value, fromBalance,
                        toBalance, addressBalance, name, symbol, decimals, blockTime);

                logErcList.add(logErc);
            }
        }

        return logErcList;
    }
}
