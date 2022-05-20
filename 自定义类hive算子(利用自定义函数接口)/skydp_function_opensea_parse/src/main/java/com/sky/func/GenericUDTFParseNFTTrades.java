package com.sky.func;

import com.alibaba.fastjson.JSON;
import com.sky.bean.log.*;
import com.sky.bean.transaction.*;
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Description(name = "explode_nft_trades()",
        value = "_FUNC_(a) - separates the elements of string a into multiple rows")

public class GenericUDTFParseNFTTrades extends GenericUDTF {
    private transient final String[] result = new String[1];
    //获取opensea的税收
    private transient final ArrayList<OpenseaFee> openseaFeeList = new ArrayList<>();
    //存储erc20,erc721, erc1155的transfer日志信息
    private transient final HashMap<String, ArrayList<Erc20Transfer>> erc20Map = new HashMap<>();
    private transient final HashMap<String, ArrayList<Erc721Transfer>> erc721Map = new HashMap<>();
    private transient final HashMap<String, ArrayList<Erc1155TransferSingle>> erc1155Map = new HashMap<>();

    //opensea钱包地址
    private final static String OPENSEA_WALLET_ADDRESS = "0x5b3256965e7c3cf26e11fcaf296dfc8807c01073";
    //空地址
    private final static String NULL_ADDRESS = "0x0000000000000000000000000000000000000000";
    //WETH合约地址
    private final static String WETH_CONTRACT_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
    //opensea_v1合约
    private final static String OPENSEA_CONTRACT_V1_ADDRESS = "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b";
    //opensea_v2合约
    private final static String OPENSEA_CONTRACT_V2_ADDRESS = "0x7f268357a8c2552623316e2562d90e642bb538e5";

    //合约方法
    private final static String ERC20_ERC721_TRANSFER_METHOD =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    private final static String ERC1155_TRANSFER_SINGLE_METHOD =
            "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
    private final static String ERC1155_TRANSFER_BATCH_METHOD =
            "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";
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
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

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
        if (txnMsg.length != 8) {
            return;
        }

        //获取交易
        Transaction transaction = new Transaction(
                txnMsg[0], txnMsg[1], txnMsg[2], txnMsg[3], txnMsg[4], txnMsg[5]
        );

        //获取inputdata
        String inputData = txnMsg[6];

        //获取log
        String logs = txnMsg[7];
        //判断是否为opensea
        if (!logs.contains(ORDERS_MATCHED_METHOD)) {
            return;
        }

        //解析所有log并进行排序
        Log[] logCases = sortLog(parseLogs(logs));
        if (logCases == null) {
            return;
        }

        //计算税收
        ParseInputDataOfOpensea parseInputDataOfOpensea = new ParseInputDataOfOpensea();
        parseInputDataOfOpensea.parseDataToFeeList(transaction.getTxTo(), inputData, openseaFeeList);

        //解析opensea
        //纪录opensea的个数
        int count = 0;
        for (int i = 0; i < logCases.length && logCases[i] != null; i++) {
            OpenSea openSea = null;

            //判断是否是transfer方法
            boolean isParse = parseLogTransfer(logCases[i]);

            //解析opensea
            if (!isParse) {
                openSea = parseOpenSea(transaction, logCases[i], count);
            }

            if (openSea != null) {
                //结果写出
                result[0] = openSea.toString();
                this.forward(result);
//                System.out.println(result[0]);
//                System.out.println("---------------------");

                //解析一个opensea count + 1
                count++;

                //清空map
                erc20Map.clear();
                erc721Map.clear();
                erc1155Map.clear();
            }
        }
        //清空list
        openseaFeeList.clear();
    }


    private OpenSea parseOpenSea(Transaction transaction, Log log, int count) {
        OpenSea openSea = new OpenSea();

        //解析ordermatch函数
        OrdersMatched order = parseOrdersMatch(log);
        if (order == null) {
            return null;
        }

        openSea.setTransaction(transaction);
        openSea.setLogIndex(log.getLogIndex());
        openSea.setExchangeContractAddress(order.getAddress());
        if (OPENSEA_CONTRACT_V1_ADDRESS.equals(openSea.getExchangeContractAddress())) {
            openSea.setPlatform("OpenSea");
            openSea.setPlatformVersion("1");
        } else if (OPENSEA_CONTRACT_V2_ADDRESS.equals(openSea.getExchangeContractAddress())) {
            openSea.setPlatform("OpenSea");
            openSea.setPlatformVersion("2");
        }

        //原始代币信息
        openSea.setOriginalAmountRaw(order.getPrice());

        //通过buyhash判断哪方是maker
        //sellhash不为null时，seller为maker，反之buyer为maker
        String orderSeller;
        String orderBuyer;
        if (!NULL_HASH.equals(order.getSellHash())) {
            openSea.setSeller(order.getMaker());
            openSea.setBuyer(transaction.getTxFrom());
            orderSeller = order.getMaker();
            orderBuyer = order.getTaker();
        } else {
            openSea.setSeller(transaction.getTxFrom());
            openSea.setBuyer(order.getMaker());
            orderSeller = order.getTaker();
            orderBuyer = order.getMaker();
        }

        //得到erc20代币信息
        getTokenMsg(openSea, orderSeller, orderBuyer);

        //得到nft信息
        getNFTMsg(openSea, orderSeller, orderBuyer);

        //得到seller税款信息
        String tokenFeeForSeller = openSea.getSeller() + "-" + OPENSEA_WALLET_ADDRESS;
        getTokenFee(openSea, tokenFeeForSeller, Side.Sell);

        //得到buyer税款信息
        String tokenFeeForBuyer  = openSea.getBuyer() + "-" + OPENSEA_WALLET_ADDRESS;
        getTokenFee(openSea, tokenFeeForBuyer, Side.Buy);

        //对于eth,获取税收
        getEthFee(openSea, count);

        return openSea;
    }

    private void getEthFee(OpenSea openSea, int count) {
        int index = 0;
        //不存在税收消息
        if(openseaFeeList.size() <= 0) {
            return;
        }

        if (count < openseaFeeList.size()) {
            index = count;
        }

        if ("ETH".equals(openSea.getOriginalCurrency())) {
            OpenseaFee openseaFee = openseaFeeList.get(index);
            //获取seller税收
            BigDecimal zero = new BigDecimal("0");
            if (openseaFee.getPlatformFeesPercentForSeller().compareTo(zero) > 0) {
                String feeValue = ToolUtil.mulBigNum(openSea.getOriginalAmount(), openseaFee.getPlatformFeesPercentForSeller().toString());
                openSea.setTokenPlatformFeesForSeller(feeValue);
                openSea.setEthPlatformFeesForSeller(feeValue);
            } else if (openseaFee.getEthPlatformFeeForSeller().compareTo(zero) > 0){
                String feeValue = ToolUtil.mulBigNum(openseaFee.getEthPlatformFeeForSeller().toString(), String.valueOf(Math.pow(10, -18)));
                openSea.setTokenPlatformFeesForSeller(feeValue);
                openSea.setEthPlatformFeesForSeller(feeValue);
            }

            //获取buyer税收
            if (openseaFee.getPlatformFeesPercentForBuyer().compareTo(zero) > 0) {
                String feeValue = ToolUtil.mulBigNum(openSea.getOriginalAmount(), openseaFee.getPlatformFeesPercentForBuyer().toString());
                openSea.setTokenPlatformFeesForBuyer(feeValue);
                openSea.setEthPlatformFeesForBuyer(feeValue);
            } else if (openseaFee.getEthPlatformFeesForBuyer().compareTo(zero) > 0){
                String feeValue = ToolUtil.mulBigNum(openseaFee.getEthPlatformFeesForBuyer().toString(), String.valueOf(Math.pow(10, -18)));
                openSea.setTokenPlatformFeesForBuyer(feeValue);
                openSea.setEthPlatformFeesForBuyer(feeValue);
            }
        }
    }


    private void getTokenFee(OpenSea openSea, String key, Side side) {
        ArrayList<Erc20Transfer> erc20Transfers = erc20Map.get(key);
        if (erc20Transfers == null || erc20Transfers.size() == 0) {
            return;
        }

        for (Erc20Transfer erc20Transfer : erc20Transfers) {
            //计算token的数量
            String tokenAmount = ToolUtil.mulBigNum(
                    erc20Transfer.getValue(), String.valueOf(Math.pow(10, -erc20Transfer.getDecimal())));
            if (side == Side.Sell) {
                String value = ToolUtil.addBigNum(openSea.getTokenPlatformFeesForSeller(), tokenAmount);
                openSea.setTokenPlatformFeesForSeller(value);
                if ("WETH".equals(openSea.getOriginalCurrency())) {
                    openSea.setEthPlatformFeesForSeller(value);
                }
            } else {
                String value = ToolUtil.addBigNum(openSea.getTokenPlatformFeesForBuyer(), tokenAmount);
                openSea.setTokenPlatformFeesForBuyer(value);
                if ("WETH".equals(openSea.getOriginalCurrency())) {
                    openSea.setEthPlatformFeesForBuyer(value);
                }
            }
        }
    }


    //得到代币信息
    private void getNFTMsg(OpenSea openSea, String orderSeller, String orderBuyer) {
        String seller = openSea.getSeller();
        String buyer = openSea.getBuyer();

        if (getErc721NFT(openSea, seller, buyer) || getErc1155NFT(openSea, seller, buyer)
                || getErc20NFT(openSea, seller, buyer)) {

            openSea.setAction("buy01");

        } else if (getErc721NFT(openSea, NULL_ADDRESS, buyer) || getErc1155NFT(openSea, NULL_ADDRESS, buyer)
                || getErc20NFT(openSea, NULL_ADDRESS, buyer)) {

            openSea.setAction("mint");

        } else if (getErc721NFT(openSea, orderSeller, orderBuyer) || getErc1155NFT(openSea, orderSeller, orderBuyer)
                || getErc20NFT(openSea, orderSeller, orderBuyer)) {

            openSea.setAction("buy01");
            openSea.setSeller(orderSeller);
            openSea.setBuyer(orderBuyer);

        } else if (getErc721NFT(openSea, NULL_ADDRESS, orderBuyer) || getErc1155NFT(openSea, NULL_ADDRESS, orderBuyer)
                || getErc20NFT(openSea, NULL_ADDRESS, orderBuyer)) {

            openSea.setAction("mint");
            openSea.setSeller(orderSeller);
            openSea.setBuyer(orderBuyer);

        } else if (getErc721NFT(openSea, seller, null) || getErc1155NFT(openSea, seller, null)
                || getErc20NFT(openSea, seller, null) || getErc721NFT(openSea, null, buyer)
                || getErc1155NFT(openSea, null, buyer) || getErc20NFT(openSea, null, buyer)) {

            openSea.setAction("buyer02");

        } else if (getErc721NFT(openSea, orderSeller, null) || getErc1155NFT(openSea, orderSeller, null)
                || getErc20NFT(openSea, orderSeller, null) || getErc721NFT(openSea, null, orderBuyer)
                || getErc1155NFT(openSea, null, orderBuyer) || getErc20NFT(openSea, null, orderBuyer)) {
            openSea.setAction("buyer02");
            openSea.setSeller(orderSeller);
            openSea.setBuyer(orderBuyer);

        }

    }


    //得到erc20 nft信息
    private boolean getErc20NFT(OpenSea openSea, String seller, String buyer) {
        String key = null;
        if (seller != null && buyer != null) {
            key = seller + '-' + buyer;
        } else if (seller != null) {
            key = seller + '-';
        } else if (buyer != null) {
            key = '-' + buyer;
        }

        ArrayList<Erc20Transfer> erc20Transfers = erc20Map.get(key);

        if (erc20Transfers != null && erc20Transfers.size() != 0) {
            openSea.setErcStandard("ERC20");

            for (Erc20Transfer erc20Transfer : erc20Transfers) {
                //拼接nft_id字符串
                openSea.setNftTokenId(openSea.getNftTokenId() + erc20Transfer.getValue() + ",");
                openSea.setNftNum(openSea.getNftNum() + 1);
                openSea.setNftProjectName(erc20Transfer.getSymbol());
                openSea.setNftContractAddress(erc20Transfer.getAddress());
            }

            String tokenIds = openSea.getNftTokenId();
            //去掉,
            if (tokenIds.length() != 0){
                openSea.setNftTokenId(tokenIds.substring(0, tokenIds.length() - 1));
            }

            return true;
        }
        return false;
    }

    //得到erc1155 nft信息
    private boolean getErc1155NFT(OpenSea openSea, String seller, String buyer) {
        String key = null;
        if (seller != null && buyer != null) {
            key = seller + '-' + buyer;
        } else if (seller != null) {
            key = seller + '-';
        } else if (buyer != null) {
            key = '-' + buyer;
        }

        ArrayList<Erc1155TransferSingle> erc1155TransferSingles = erc1155Map.get(key);
        if (erc1155TransferSingles != null && erc1155TransferSingles.size() != 0) {
            openSea.setErcStandard("ERC1155");
            for (Erc1155TransferSingle erc1155TransferSingle : erc1155TransferSingles) {
                //拼接nft_id字符串
                String[] tokenIds = erc1155TransferSingle.getTokenId().split(",");
                String[] values = erc1155TransferSingle.getValue().split(",");

                StringBuilder tokens = new StringBuilder();
                for (int i = 0; i < tokenIds.length && i < values.length; i++) {
                    tokens.append(tokenIds[i]).append("-").append(values[i]).append(",");
                }

                openSea.setNftTokenId(openSea.getNftTokenId() + tokens);
                openSea.setNftNum(openSea.getNftNum() + tokenIds.length);
                openSea.setNftProjectName(erc1155TransferSingle.getSymbol());
                openSea.setNftContractAddress(erc1155TransferSingle.getAddress());
            }

            String tokenIds = openSea.getNftTokenId();
            //去掉,
            if (tokenIds.length() != 0) {
                openSea.setNftTokenId(tokenIds.substring(0, tokenIds.length() - 1));
            }
            return true;
        }
        return false;
    }


    //得到erc721 nft信息
    private boolean getErc721NFT(OpenSea openSea, String seller, String buyer) {
        String key = null;
        if (seller != null && buyer != null) {
            key = seller + '-' + buyer;
        } else if (seller != null) {
            key = seller + '-';
        } else if (buyer != null) {
            key = '-' + buyer;
        }

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
            if (tokenIds.length() != 0) {
                openSea.setNftTokenId(tokenIds.substring(0, tokenIds.length() - 1));
            }
            return true;
        }

        return false;
    }


    //得到代币值
    private void getTokenMsg(OpenSea openSea, String orderSeller, String orderBuyer) {

        if (getErc20TokenMsg(openSea, openSea.getBuyer() + "-" + openSea.getSeller())) {
            return;
        } else if (getErc20TokenMsg(openSea, orderBuyer + '-' + orderSeller)) {
            return;
        }

        //获取ETH的币价
        String ethValue = ToolUtil.mulBigNum(
                openSea.getOriginalAmountRaw(), String.valueOf(Math.pow(10, -18))
        );
        //计算token的值，去精度
        openSea.setOriginalAmount(ethValue);
        //token的名字
        openSea.setOriginalCurrency("ETH");
        //token的合约地址
        openSea.setOriginalCurrencyContract(NULL_ADDRESS);
        openSea.setCurrencyContract(WETH_CONTRACT_ADDRESS);
        openSea.setEthAmount(ethValue);
    }

    //得到代币信息
    private boolean getErc20TokenMsg(OpenSea openSea, String key) {
        ArrayList<Erc20Transfer> erc20Transfers = erc20Map.get(key);
        if (erc20Transfers == null || erc20Transfers.size() == 0) {
            return false;
        }

        for (Erc20Transfer erc20Transfer : erc20Transfers) {
            if (openSea.getOriginalAmountRaw() != null && openSea.getOriginalAmountRaw().equals(erc20Transfer.getValue())) {
                //计算token的值，去精度
                String tokenValue = ToolUtil.mulBigNum(openSea.getOriginalAmountRaw(), String.valueOf(Math.pow(10, -erc20Transfer.getDecimal())));
                //设置币价格
                openSea.setOriginalAmount(tokenValue);
                //token的名字
                if (WETH_CONTRACT_ADDRESS.equals(erc20Transfer.getAddress())) {
                    openSea.setOriginalCurrency("WETH");
                    openSea.setEthAmount(tokenValue);
                }
                //token的合约地址
                openSea.setOriginalCurrencyContract(erc20Transfer.getAddress());
                openSea.setCurrencyContract(erc20Transfer.getAddress());
                break;
            }
        }

        return true;
    }


    //解析orderMatched
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
        } else if (parseErc20AndErc721Transfer(log)) {
            return true;
        } else {
            return parseErc1155Transfer(log);
        }
    }

    //解析erc1155transfer
    private boolean parseErc1155Transfer(Log log) {
        Object[] topics = log.getTopics();
        String method = topics[0].toString();

        if ((ERC1155_TRANSFER_SINGLE_METHOD.equals(method) || ERC1155_TRANSFER_BATCH_METHOD.equals(method))
                && topics.length == 4) {
            //去掉0x
            String data = Numeric.cleanHexPrefix(log.getData());
            if (data == null || data.length() == 0) {
                return false;
            }

            //解析出tokenId和value
            StringBuilder tokenId = new StringBuilder();
            StringBuilder value = new StringBuilder();

            //transferSingle
            if (ERC1155_TRANSFER_SINGLE_METHOD.equals(method)) {
                if (data.length() == 128) {
                    tokenId = new StringBuilder(ToolUtil.hexToNumStr(data.substring(0, 64)));
                    value = new StringBuilder(ToolUtil.hexToNumStr(data.substring(64)));
                }
            } else {  //transferBatch
                int index = 2 * 64;

                //获取token
                long tokensNum = new Long(ToolUtil.hexToNumStr(data.substring(index, index + 64)));
                index += 64;
                for (int i = 0; i < tokensNum; i++) {
                    tokenId.append(ToolUtil.hexToNumStr(data.substring(index, index + 64))).append(",");
                    index += 64;
                }

                if (tokenId.length() != 0) {
                    tokenId = new StringBuilder(tokenId.substring(0, tokenId.length() - 1));
                }

                //获取value
                long valuesCount = new Long(ToolUtil.hexToNumStr(data.substring(index, index + 64)));
                index += 64;
                for (int i = 0; i < valuesCount; i++) {
                    value.append(ToolUtil.hexToNumStr(data.substring(index, index + 64))).append(",");
                    index += 64;
                }

                if (value.length() != 0) {
                    value = new StringBuilder(value.substring(0, value.length() - 1));
                }

            }

            Erc1155TransferSingle erc1155Case = new Erc1155TransferSingle(
                    log.getAddress(),  //address
                    log.getSymBol(),    //symbol
                    ToolUtil.str66To42(topics[2].toString()),  //from
                    ToolUtil.str66To42(topics[3].toString()),  //to
                    tokenId.toString(),
                    value.toString()
            );

            //数据写入map中
            erc1155Map.computeIfAbsent(erc1155Case.getFrom() + '-', k -> new ArrayList<>()).add(erc1155Case);
            erc1155Map.computeIfAbsent('-' + erc1155Case.getTo(), k -> new ArrayList<>()).add(erc1155Case);

            String key = erc1155Case.getFrom() + "-" + erc1155Case.getTo();
            erc1155Map.computeIfAbsent(key, k -> new ArrayList<>()).add(erc1155Case);
            return true;
        }

        return false;
    }

    //解析erc20&erc721
    private boolean parseErc20AndErc721Transfer(Log log) {
        Object[] topics = log.getTopics();
        String method = topics[0].toString();

        if (ERC20_ERC721_TRANSFER_METHOD.equals(method)) {
            if (topics.length == 4) {

                Erc721Transfer erc721Case = new Erc721Transfer(
                        log.getAddress(),
                        log.getSymBol(),
                        ToolUtil.str66To42(topics[1].toString()),  //from
                        ToolUtil.str66To42(topics[2].toString()),  //too
                        ToolUtil.hexToNumStr(topics[3].toString())  //token_id
                );

                //数据写入map中
                erc721Map.computeIfAbsent(erc721Case.getFrom() + '-', k -> new ArrayList<>()).add(erc721Case);
                erc721Map.computeIfAbsent('-' + erc721Case.getTo(), k -> new ArrayList<>()).add(erc721Case);

                String key = erc721Case.getFrom() + "-" + erc721Case.getTo();
                erc721Map.computeIfAbsent(key, k -> new ArrayList<>()).add(erc721Case);
                return true;
            } else {
                String from, to, value;
                if (topics.length == 1) {
                    from = ToolUtil.str66To42(log.getData().substring(0, 64));  //from
                    to = ToolUtil.str66To42(log.getData().substring(64, 128)); //to
                    value = ToolUtil.hexToNumStr(log.getData().substring(128)); //value
                } else {
                    from = ToolUtil.str66To42(topics[1].toString());  //from
                    to = ToolUtil.str66To42(topics[2].toString());  //to
                    value = ToolUtil.hexToNumStr(log.getData());  //value
                }

                Erc20Transfer erc20Case = new Erc20Transfer(
                        log.getAddress(),
                        log.getSymBol(),
                        log.getDecimal(),
                        from,
                        to,
                        value
                );

                //数据写入map中
                erc20Map.computeIfAbsent(erc20Case.getFrom() + '-', k -> new ArrayList<>()).add(erc20Case);
                erc20Map.computeIfAbsent('-' + erc20Case.getTo(), k -> new ArrayList<>()).add(erc20Case);

                String key = erc20Case.getFrom() + "-" + erc20Case.getTo();
                erc20Map.computeIfAbsent(key, k -> new ArrayList<>()).add(erc20Case);
                return true;
            }
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
            String[] logArr = logs[i].split("=");

            //log的长度需要为7 logindex=-1不是etherscan日志
            if (logArr.length == 6 && !"-1".equals(logArr[5])) {
                logList.add(new Log(
                        logArr[0],     //address
                        JSON.parseArray(logArr[1]).toArray(), //topics
                        logArr[2],  //data
                        logArr[3],  //ercType
                        "",  //symbol
                        Integer.parseInt(!"".equals(logArr[4]) ? logArr[4] : "0"),  //decimal
                        Integer.parseInt(logArr[5])  //logindex
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