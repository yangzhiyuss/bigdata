package com.sky.func;

import com.alibaba.fastjson.JSON;
import com.sky.bean.*;
import com.sky.util.ToolUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.web3j.utils.Numeric;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Description(name = "explode_nft_trades()",
        value = "_FUNC_(a) - separates the elements of string a into multiple rows")


public class GenericUDTFParseNFTTrades extends GenericUDTF {
    private transient final String[] result = new String[1];
    //存储erc20,erc721, erc1155的transfer日志信息
    private transient final HashMap<String, ArrayList<Erc20Transfer>> erc20Map = new HashMap<>();
    private transient final HashMap<String, ArrayList<Erc721Transfer>> erc721Map = new HashMap<>();
    private transient final HashMap<String, ArrayList<Erc1155TransferSingle>> erc1155Map = new HashMap<>();

    //opensea钱包地址
    private final static String OPENSEA_WALLET_ADDRESS = "0x5b3256965e7c3cf26e11fcaf296dfc8807c01073";
    //空地址
    private final static String ETH_CONTRACT_ADDRESS = "0x0000000000000000000000000000000000000000";
    //WETH合约地址
    private final static String WETH_CONTRACT_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";


    //合约方法
    private final static String ERC20_ERC721_TRANSFER_METHOD =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    private final static String ERC1155_TRANSFER_SINGLE_METHOD =
            "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
    private final static String ORDERS_MATCHED_METHOD =
            "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9";
    private final static String NULL_HASH =
            "0000000000000000000000000000000000000000000000000000000000000000";


    public GenericUDTFParseNFTTrades() {

    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // 1 参数合法性检查
        if (argOIs.length != 1) {
            throw new UDFArgumentException("explode_nft_trades() takes only one argument");
        }

        // 2 第一个参数必须为string
        //判断参数是否为基础数据类型
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("explode_nft_trades()  accepts only basic type parameters ");
        }

        //将参数对象检查器强转为基础类型对象检查器
        PrimitiveObjectInspector argumentOI = (PrimitiveObjectInspector) argOIs[0];

        //判断参数是否为String类型
        if (argumentOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("explode_nft_trades() accepts only string type parameters");
        }

        // 3 定义返回值名称和类型
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("col_e");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }


    @Override
    public void process(Object[] args) throws HiveException {
        //对参数进行限制
        if (args == null || args.length == 0 || args[0] == null) {
            return;
        }

        //String进行切割
        String txn = args[0].toString();
        String[] txnMsg = txn.split("#");

        //判断日志是否为空
        if (txnMsg.length != 7) {
            return;
        }

        //获取交易
        Transaction transaction = new Transaction(
                txnMsg[0], txnMsg[1], txnMsg[2], txnMsg[3], txnMsg[4], txnMsg[5]
        );

        //解析所有log并进行排序
        String logs = txnMsg[6];
        Log[] logCases = sortLog(parseLogs(logs));
        if (logCases == null) {
            return;
        }

        //解析opensea
        for (int i = 0; i < logCases.length && logCases[i] != null; i++) {
            OpenSea openSea = null;

            //判断是否是transfer方法
            boolean isSuccess = parseLogTransfer(logCases[i]);

            if (!isSuccess) {
                openSea = parseOpenSea(transaction, logCases[i]);
            }

            if (openSea != null) {
                //结果写出
                result[0] = openSea.toString();
                this.forward(result);
//                System.out.println(result[0]);
//                System.out.println("---------------------");

                //清空map
                erc20Map.clear();
                erc721Map.clear();
                erc1155Map.clear();
            }

        }

    }


    private OpenSea parseOpenSea(Transaction transaction, Log log) {
        OpenSea openSea = new OpenSea();

        //解析ordermatch函数
        OrdersMatched order = parseOrdersMatch(log);
        if (order == null) {
            return null;
        }

        openSea.setTransaction(transaction);
        openSea.setLogIndex(log.getLogIndex());
        openSea.setPlatform("OpenSea");
        openSea.setPlatformVersion("1");
        openSea.setExchangeContractAddress(order.getAddress());
        //原始代币信息
        openSea.setOriginalAmountRaw(order.getPrice());

        //通过buyhash判断哪方是maker
        //buyhash为null时，seller为maker，反之buyer为maker
        if (NULL_HASH.equals(order.getBuyHash())) {
            openSea.setSeller(order.getMaker());
            openSea.setBuyer(transaction.getTxFrom());
        } else {
            openSea.setSeller(transaction.getTxFrom());
            openSea.setBuyer(order.getMaker());
        }

        //获取key去匹配map
        String buyNFTKey = openSea.getBuyer() + "-" + openSea.getSeller();
        String sellNFTKey = openSea.getSeller() + "-" + openSea.getBuyer();
        String tokenFeeForBuyer  = openSea.getBuyer() + "-" + OPENSEA_WALLET_ADDRESS;
        String tokenFeeForSeller = openSea.getSeller() + "-" + OPENSEA_WALLET_ADDRESS;

        //得到erc20代币信息
        getTokenMsg(openSea, buyNFTKey);
        //得到nft信息
        getNFTMsg(openSea, sellNFTKey);
        //得到seller税款信息
        getFeeForSeller(openSea, tokenFeeForSeller);
        //得到buyer税款信息
        getFeeForBuyer(openSea, tokenFeeForBuyer);

        return openSea;
    }

    private void getFeeForSeller(OpenSea openSea, String key) {
        ArrayList<Erc20Transfer> erc20Transfers = erc20Map.get(key);
        if (erc20Transfers == null || erc20Transfers.size() == 0) {
            return;
        }

        for (Erc20Transfer erc20Transfer : erc20Transfers) {
            //计算token的数量
            String tokenAmount = ToolUtil.mulBigNum(
                    erc20Transfer.getValue(), String.valueOf(Math.pow(10, -erc20Transfer.getDecimal())));

            String value = ToolUtil.addBigNum(openSea.getTokenPlatformFeesForSeller(), tokenAmount);
            openSea.setTokenPlatformFeesForSeller(value);
        }
    }


    private void getFeeForBuyer(OpenSea openSea, String key) {
        ArrayList<Erc20Transfer> erc20Transfers = erc20Map.get(key);
        if (erc20Transfers == null || erc20Transfers.size() == 0) {
            return;
        }

        for (Erc20Transfer erc20Transfer : erc20Transfers) {
            //计算token的数量
            String tokenAmount = ToolUtil.mulBigNum(
                    erc20Transfer.getValue(), String.valueOf(Math.pow(10, -erc20Transfer.getDecimal())));

            String value = ToolUtil.addBigNum(openSea.getTokenPlatformFeesForBuyer(), tokenAmount);
            openSea.setTokenPlatformFeesForBuyer(value);
        }
    }


    private void getNFTMsg(OpenSea openSea, String key) {
        //erc721代币
        ArrayList<Erc721Transfer> erc721Transfers = erc721Map.get(key);

        if (erc721Transfers != null && erc721Transfers.size() != 0) {
            openSea.setErcStandard("ERC721");
            for (Erc721Transfer erc721Transfer : erc721Transfers) {
                openSea.setNftTokenId(openSea.getNftTokenId() + erc721Transfer.getTokenId() + ',');
                openSea.setNftNum(openSea.getNftNum() + 1 );
                openSea.setNftProjectName(erc721Transfer.getSymbol());
                openSea.setNftContractAddress(erc721Transfer.getAddress());
            }
            String tokenIds = openSea.getNftTokenId();
            openSea.setNftTokenId(tokenIds.substring(0, tokenIds.length() - 1));
        }

        //erc1155代币
        ArrayList<Erc1155TransferSingle> erc1155TransferSingles = erc1155Map.get(key);

        if (erc1155TransferSingles != null && erc1155TransferSingles.size() != 0) {
            openSea.setErcStandard("ERC1155");
            for (Erc1155TransferSingle erc1155TransferSingle : erc1155TransferSingles) {
                //拼接nft_id字符串
                openSea.setNftTokenId(
                        openSea.getNftTokenId() + erc1155TransferSingle.getTokenId() + "-" + erc1155TransferSingle.getValue() + ","
                );
                openSea.setNftNum(openSea.getNftNum() + 1);
                openSea.setNftProjectName(erc1155TransferSingle.getSymbol());
                openSea.setNftContractAddress(erc1155TransferSingle.getAddress());
            }

            String tokenIds = openSea.getNftTokenId();
            //去掉,
            openSea.setNftTokenId(tokenIds.substring(0, tokenIds.length() - 1));
        }

    }


    private void getTokenMsg(OpenSea openSea, String key) {
        ArrayList<Erc20Transfer> erc20Transfers = erc20Map.get(key);

        if (erc20Transfers != null && erc20Transfers.size() != 0) {

            for (Erc20Transfer erc20Transfer : erc20Transfers) {
                if (openSea.getOriginalAmountRaw() != null && openSea.getOriginalAmountRaw().equals(erc20Transfer.getValue())) {
                    //计算token的值，去精度
                    openSea.setOriginalAmount(
                            ToolUtil.mulBigNum(
                                    openSea.getOriginalAmountRaw(), String.valueOf(Math.pow(10, -erc20Transfer.getDecimal()))
                            )
                    );
                    //token的名字
                    openSea.setOriginalCurrency(erc20Transfer.getSymbol());
                    //token的合约地址
                    openSea.setOriginalCurrencyContract(erc20Transfer.getAddress());
                    openSea.setCurrencyContract(erc20Transfer.getAddress());
                    break;
                }
            }

        } else {
            String ethValue = ToolUtil.mulBigNum(
                    openSea.getOriginalAmountRaw(), String.valueOf(Math.pow(10, -18))
            );
            //计算token的值，去精度
            openSea.setOriginalAmount(ethValue);
            //token的名字
            openSea.setOriginalCurrency("ETH");
            //token的合约地址
            openSea.setOriginalCurrencyContract(ETH_CONTRACT_ADDRESS);
            openSea.setCurrencyContract(WETH_CONTRACT_ADDRESS);
            openSea.setEthAmount(ethValue);
        }

    }


    private OrdersMatched parseOrdersMatch(Log log) {
        Object[] topics = log.getTopics();

        //判断参数是否合法
        if (topics == null || topics.length == 0) {
            return null;
        }

        //判断是否为opensea
        String method = topics[0].toString();
        if (!ORDERS_MATCHED_METHOD.equals(method) || topics.length != 4) {
            return null;
        }

        //解析topic
        String topic1 = ToolUtil.str66To42(topics[1].toString());
        String topic2 = ToolUtil.str66To42(topics[2].toString());
        //解析data
        //去掉0x
        String data = Numeric.cleanHexPrefix(log.getData());
        //解析出tokenId和value
        String buyHash = data.substring(0, 64);
        String sellHash = data.substring(64, 128);
        String price = ToolUtil.hexToNumStr(data.substring(128));

        return new OrdersMatched(log.getAddress(), topic1, topic2, buyHash, sellHash, price);
    }


    //解析transfer,包括erc20,erc721,erc1155
    private boolean parseLogTransfer(Log log) {
        Object[] topics = log.getTopics();

        if (topics == null || topics.length == 0) {
            return false;
        }

        String method = topics[0].toString();
        if (ERC20_ERC721_TRANSFER_METHOD.equals(method)) {
            if (topics.length == 3) {
                Erc20Transfer erc20Case = new Erc20Transfer(
                        log.getAddress(),
                        log.getSymBol(),
                        log.getDecimal(),
                        ToolUtil.str66To42(topics[1].toString()),
                        ToolUtil.str66To42(topics[2].toString()),
                        ToolUtil.hexToNumStr(log.getData())
                );

                //数据写入map中
                String key = erc20Case.getFrom() + "-" + erc20Case.getTo();
                erc20Map.computeIfAbsent(key, k -> new ArrayList<>()).add(erc20Case);
                return true;

            } else if (topics.length == 4) {
                Erc721Transfer erc721Case = new Erc721Transfer(
                        log.getAddress(),
                        log.getSymBol(),
                        ToolUtil.str66To42(topics[1].toString()),
                        ToolUtil.str66To42(topics[2].toString()),
                        ToolUtil.hexToNumStr(topics[3].toString())
                );

                //数据写入map中
                String key = erc721Case.getFrom() + "-" + erc721Case.getTo();
                erc721Map.computeIfAbsent(key, k -> new ArrayList<>()).add(erc721Case);
                return true;
            }

        } else if (ERC1155_TRANSFER_SINGLE_METHOD.equals(method) && topics.length == 4) {
            //去掉0x
            String data = Numeric.cleanHexPrefix(log.getData());
            //解析出tokenId和value
            String tokenId = ToolUtil.hexToNumStr(data.substring(0, 64));
            String value = ToolUtil.hexToNumStr(data.substring(64));
            Erc1155TransferSingle erc1155Case = new Erc1155TransferSingle(
                    log.getAddress(),
                    log.getSymBol(),
                    ToolUtil.str66To42(topics[2].toString()),
                    ToolUtil.str66To42(topics[3].toString()),
                    tokenId,
                    Integer.parseInt(value)
            );

            //数据写入map中
            String key = erc1155Case.getFrom() + "-" + erc1155Case.getTo();
            erc1155Map.computeIfAbsent(key, k -> new ArrayList<>()).add(erc1155Case);
            return true;

        }
        return false;
    }


    //字符串转化成log结构
    private ArrayList<Log> parseLogs(String param) {
        ArrayList<Log> logList = new ArrayList<>();
        //参数为空返回null
        if (param == null) {
            return null;
        }
        //切割参数
        String[] logs = param.split("_");
        //赋值
        for (int i = 0; i < logs.length; i++) {
            String[] logArr = logs[i].split("-");

            //log的长度需要为7 logindex=-1不是etherscan日志
            if (logArr.length == 7 && !"-1".equals(logArr[6])) {
                logList.add(new Log(
                        logArr[0],
                        JSON.parseArray(logArr[1]).toArray(),
                        logArr[2],
                        logArr[3],
                        logArr[4],
                        Integer.parseInt(!"".equals(logArr[5]) ? logArr[5] : "0"),
                        Integer.parseInt(logArr[6])
                ));
            }
        }
        return logList;
    }


    //按升序排列 log_index是紧密排序的，找出最小值，然后logIndex-min就能确定log存放的位置，算法复杂度为O(n)
    private Log[] sortLog(ArrayList<Log> params) {
        if (params == null || params.size() == 0) {
            return null;
        }

        Log[] logs = new Log[params.size()];

        int min = Integer.MAX_VALUE;
        for (Log param : params) {
            min = Math.min(min, param.getLogIndex());
        }

        for (Log param : params) {
            int index = param.getLogIndex() - min;
            //防止数组越界
            if (index >= 0 && index < params.size()) {
                logs[index] = param;
            }
        }
        return logs;
    }

    @Override
    public void close() throws HiveException {

    }
}