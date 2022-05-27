package com.skydp.dws.nfttrades;

import com.skydp.bean.dws.transaction.*;
import com.skydp.utils.ToolUtil;
import org.web3j.utils.Numeric;

import java.math.BigDecimal;
import java.util.ArrayList;

public class ParseInputDataOfOpensea {
    //opensea_v1合约
    private final static String OPENSEA_CONTRACT_V1_ADDRESS = "7be8076f4ea4a4ad08075c2508e481d6c946d12b";
    //opensea_v2合约
    private final static String OPENSEA_CONTRACT_V2_ADDRESS = "7f268357a8c2552623316e2562d90e642bb538e5";
    //空地址
    private final static String NULL_ADDRESS = "0x0000000000000000000000000000000000000000";
    //汇率除数
    private final static BigDecimal INVERSE_BASIS_POINT = new BigDecimal(10000);

    public void parseDataToFeeList(String to, String args, ArrayList<OpenseaFee> openseaFeeList) {
        //判断参数是否合法
        if (args == null || args.length() == 0) {
            return;
        }

        //去掉0x
        String data = Numeric.cleanHexPrefix(args);
        if (data.length() < 8) {
            return;
        }

        //判断opensea的地址
        String openseaAddress = "";
        if (data.contains(OPENSEA_CONTRACT_V2_ADDRESS)) {
            openseaAddress = OPENSEA_CONTRACT_V2_ADDRESS;
        } else if (data.contains(OPENSEA_CONTRACT_V1_ADDRESS)) {
            openseaAddress = OPENSEA_CONTRACT_V1_ADDRESS;
        }

        //获取索引
        int index = data.indexOf(openseaAddress);
        while (index != -1 && !"".equals(openseaAddress)) {
            //切割字符串，地址字符位64位，地址40位,补24位,从opensea的参数开始解
            String params = data.substring(index - 24);
            //获得inputdata
            InputData inputData = parseInputData(params);

            //如果解析成功automicmatch
            if (inputData != null) {
                //解析automicMatch
                openseaFeeList.add(parseOpenSeaData(inputData));
                //判断数据是否取完
                if (inputData.getLastIndex() == data.length()) {
                    break;
                }
                //解析后面的数据
                data = params;
                index = data.indexOf(openseaAddress, inputData.getLastIndex());
            } else {
                //往后移动
                data = params;
                index = data.indexOf(openseaAddress, 64);
            }
        }

        if (openseaFeeList.size() == 0 && "".equals(openseaAddress)) {
            //解析一些特定的不能解析的合约
            parseSpecialOpenseaData(to, args, openseaFeeList);
        }
    }

    private void parseSpecialOpenseaData(String to, String args, ArrayList<OpenseaFee> openseaFeeList) {
        String data = Numeric.cleanHexPrefix(args);

        OpenseaFee openseaFee = new OpenseaFee();
        if ("0x0000000031f7382a812c64b604da4fc520afef4b".equals(to)) {
            //获取税收地址
            BigDecimal sellFeePercent =
                    ToolUtil.strToBigDecimal(ToolUtil.hexToNumStr(data.substring(42, 46))).divide(INVERSE_BASIS_POINT);

            openseaFee.setPlatformFeesPercentForSeller(sellFeePercent);
            //写入
            openseaFeeList.add(openseaFee);
        }
        else if ("0x0000000035634b55f3d99b071b5a354f48e10bef".equals(to) ||
                "0x00000000a50bb64b4bbeceb18715748dface08af".equals(to)) {
            //获取税收地址
            BigDecimal sellFeePercent =
                    ToolUtil.strToBigDecimal(ToolUtil.hexToNumStr(data.substring(40, 44))).divide(INVERSE_BASIS_POINT);

            openseaFee.setPlatformFeesPercentForSeller(sellFeePercent);
            //写入
            openseaFeeList.add(openseaFee);
        } else if ("0x2af4b707e1dce8fc345f38cfeeaa2421e54976d5".equals(to)) {
            //获取税收地址
            BigDecimal sellFeePercent =
                    ToolUtil.strToBigDecimal(ToolUtil.hexToNumStr(data.substring(141, 144))).divide(INVERSE_BASIS_POINT);
            openseaFee.setPlatformFeesPercentForSeller(sellFeePercent);
            //写入
            openseaFeeList.add(openseaFee);
        }
    }

    private OpenseaFee parseOpenSeaData(InputData inputData) {
        //获取order
        Order buy = new Order(inputData.getAddress()[0], inputData.getAddress()[1], inputData.getAddress()[2],
                ToolUtil.strToBigDecimal(inputData.getUints()[0]), ToolUtil.strToBigDecimal(inputData.getUints()[1]),
                ToolUtil.strToBigDecimal(inputData.getUints()[2]), ToolUtil.strToBigDecimal(inputData.getUints()[3]),
                inputData.getAddress()[3],
                inputData.getFeeMethodsSidesKindsHowToCalls()[0] == 0? FeeMethod.ProtocolFee: FeeMethod.SplitFee,
                inputData.getFeeMethodsSidesKindsHowToCalls()[1] == 0? Side.Buy: Side.Sell,
                inputData.getFeeMethodsSidesKindsHowToCalls()[2] == 0? SaleKind.FixedPrice: SaleKind.DutchAuction,
                inputData.getAddress()[4],
                inputData.getFeeMethodsSidesKindsHowToCalls()[3] == 0? HowToCall.Call: HowToCall.DelegateCall,
                inputData.getCalldataBuy(), inputData.getReplacementPatterBuy(), inputData.getAddress()[5],
                inputData.getStaticExtradataBuy(), inputData.getAddress()[6], inputData.getUints()[4],
                inputData.getUints()[5], inputData.getUints()[6], inputData.getUints()[7], inputData.getUints()[8]);

        Order sell = new Order(inputData.getAddress()[7], inputData.getAddress()[8], inputData.getAddress()[9],
                ToolUtil.strToBigDecimal(inputData.getUints()[9]), ToolUtil.strToBigDecimal(inputData.getUints()[10]),
                ToolUtil.strToBigDecimal(inputData.getUints()[11]), ToolUtil.strToBigDecimal(inputData.getUints()[12]),
                inputData.getAddress()[10],
                inputData.getFeeMethodsSidesKindsHowToCalls()[4] == 0? FeeMethod.ProtocolFee: FeeMethod.SplitFee,
                inputData.getFeeMethodsSidesKindsHowToCalls()[5] == 0? Side.Buy: Side.Sell,
                inputData.getFeeMethodsSidesKindsHowToCalls()[6] == 0? SaleKind.FixedPrice: SaleKind.DutchAuction,
                inputData.getAddress()[11],
                inputData.getFeeMethodsSidesKindsHowToCalls()[7] == 0? HowToCall.Call: HowToCall.DelegateCall,
                inputData.getCalldataSell(), inputData.getReplacementPatterSell(), inputData.getAddress()[12],
                inputData.getStaticExtradataSell(), inputData.getAddress()[13], inputData.getUints()[13],
                inputData.getUints()[14], inputData.getUints()[15], inputData.getUints()[16], inputData.getUints()[17]);

        return parseOpenseaFee(buy, sell);
    }

    private OpenseaFee parseOpenseaFee(Order buy, Order sell) {
        OpenseaFee openseaFee = new OpenseaFee();

        BigDecimal zero = new BigDecimal("0");
        if (!NULL_ADDRESS.equals(sell.getFeeRecipient())) {
            if (sell.getFeeMethod() == FeeMethod.SplitFee) {
                if (sell.getMakerRelayerFee().compareTo(zero) > 0) {
                    BigDecimal percent = sell.getMakerRelayerFee().divide(INVERSE_BASIS_POINT);
                    openseaFee.setPlatformFeesPercentForSeller(
                            openseaFee.getPlatformFeesPercentForSeller().add(percent));
                }

                if (sell.getTakerRelayerFee().compareTo(zero) > 0) {
                    BigDecimal percent = sell.getTakerRelayerFee().divide(INVERSE_BASIS_POINT);
                    openseaFee.setPlatformFeesPercentForBuyer(
                            openseaFee.getPlatformFeesPercentForBuyer().add(percent));
                }

                if (sell.getMakerProtocolFee().compareTo(zero) > 0) {
                    BigDecimal percent = sell.getMakerProtocolFee().divide(INVERSE_BASIS_POINT);
                    openseaFee.setPlatformFeesPercentForSeller(
                            openseaFee.getPlatformFeesPercentForSeller().add(percent));
                }

                if (sell.getTakerProtocolFee().compareTo(zero) > 0) {
                    BigDecimal percent = sell.getTakerProtocolFee().divide(INVERSE_BASIS_POINT);
                    openseaFee.setPlatformFeesPercentForBuyer(
                            openseaFee.getPlatformFeesPercentForBuyer().add(percent));
                }
            } else {
                openseaFee.setEthPlatformFeeForSeller(
                        openseaFee.getEthPlatformFeeForSeller().add(sell.getMakerRelayerFee()));

                openseaFee.setEthPlatformFeesForBuyer(
                        openseaFee.getEthPlatformFeesForBuyer().add(sell.getTakerRelayerFee()));
            }
        } else {
            if (sell.getFeeMethod() == FeeMethod.SplitFee) {
                if (NULL_ADDRESS.equals(sell.getPaymentToken()))
                    return openseaFee;

                if (buy.getMakerRelayerFee().compareTo(zero) > 0) {
                    BigDecimal percent = buy.getMakerRelayerFee().divide(INVERSE_BASIS_POINT);
                    openseaFee.setPlatformFeesPercentForBuyer(
                            openseaFee.getPlatformFeesPercentForBuyer().add(percent));
                }

                if (buy.getTakerRelayerFee().compareTo(zero) > 0) {
                    BigDecimal percent = buy.getTakerRelayerFee().divide(INVERSE_BASIS_POINT);
                    openseaFee.setPlatformFeesPercentForSeller(
                            openseaFee.getPlatformFeesPercentForSeller().add(percent));
                }

                if (buy.getMakerProtocolFee().compareTo(zero) > 0) {
                    BigDecimal percent = buy.getMakerProtocolFee().divide(INVERSE_BASIS_POINT);
                    openseaFee.setPlatformFeesPercentForBuyer(
                            openseaFee.getPlatformFeesPercentForBuyer().add(percent));
                }

                if (buy.getTakerProtocolFee().compareTo(zero) > 0) {
                    BigDecimal percent = buy.getTakerProtocolFee().divide(INVERSE_BASIS_POINT);
                    openseaFee.setPlatformFeesPercentForSeller(
                            openseaFee.getPlatformFeesPercentForSeller().add(percent));
                }


            } else {
                openseaFee.setEthPlatformFeesForBuyer(
                        openseaFee.getEthPlatformFeesForBuyer().add(buy.getMakerRelayerFee())
                );
                openseaFee.setPlatformFeesPercentForSeller(
                     openseaFee.getEthPlatformFeeForSeller().add(buy.getTakerRelayerFee())
                );
            }
        }

        return openseaFee;
    }

    private InputData parseInputData(String params) {
        InputData inputData = new InputData();
        //params索引
        int index = 0;
        //解析inputData
        String[] address = inputData.getAddress();
        for (int i = 0; i < address.length && index + 64 <= params.length(); i++, index += 64) {
            try {
                address[i] = ToolUtil.str66To42(params.substring(index, index + 64));
            } catch (NullPointerException e) {
                return null;
            }

        }

        String[] uints = inputData.getUints();
        for (int i = 0; i < uints.length && index + 64 <= params.length(); i++, index += 64) {
            try {
                uints[i] = ToolUtil.hexToNumStr(params.substring(index, index + 64));
            } catch (NullPointerException e) {
                return null;
            }
        }

        long[] feeMethodsSidesKindsHowToCalls = inputData.getFeeMethodsSidesKindsHowToCalls();
        for (int i = 0; i < feeMethodsSidesKindsHowToCalls.length && index + 64 < params.length(); i++, index += 64) {
            try {
                feeMethodsSidesKindsHowToCalls[i] = Long.parseLong(params.substring(index, index + 64), 16);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        //解析bytes字段
        index = parseBytes(inputData, params, index);
        if (inputData.getLastIndex() == -1) {
            return null;
        }

        long[] vs = inputData.getVs();
        for (int i = 0; i < vs.length && index + 64 <= params.length(); i++, index += 64) {
            try {
                vs[i] = Long.parseLong(params.substring(index, index + 64), 16);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        String[] rssMetadata = inputData.getRssMetadata();
        for (int i = 0; i < rssMetadata.length && index + 64 <= params.length(); i++, index += 64) {
            try {
                rssMetadata[i] = params.substring(index, index + 64);
            } catch (NullPointerException e) {
                return null;
            }
        }

        return inputData;
    }

    private int parseBytes (InputData inputData, String params, int index) {
        String[] bytes = new String[6];

        //获取buyes数据
        int end = -1;
        for (int i = 0; i < bytes.length && index + 64 <= params.length(); i++, index += 64) {
            //获取数据所在的位置
            try {
                int paramsIndex = Integer.parseInt(params.substring(index, index + 64), 16) * 2;
                if (paramsIndex + 64 <= params.length()) {
                    //获取bytes的长度 十六位 字节 *  2
                    int length = Integer.parseInt(params.substring(paramsIndex, paramsIndex + 64), 16) * 2;
                    //开始解析
                    int start = paramsIndex + 64;
                    end = start + length;
                    if (end  <= params.length()) {
                        bytes[i] = params.substring(start, end);
                    }
                }
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        inputData.setCalldataBuy(bytes[0]);
        inputData.setCalldataSell(bytes[1]);
        inputData.setReplacementPatterBuy(bytes[2]);
        inputData.setReplacementPatterSell(bytes[3]);
        inputData.setStaticExtradataBuy(bytes[4]);
        inputData.setStaticExtradataSell(bytes[5]);
        inputData.setLastIndex(end);

        return index;
    }
}
