package com.sky.util;

import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.web3j.utils.Numeric;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

public class ToolUtil {
    //十六进制转化成十进制
    public static String hexToNumStr(String param) {
        return Numeric.toBigInt(param).toString();
    }

    //长度为64的字符串转化为42位的address
    public static String str66To42(String param) {
        return Numeric.prependHexPrefix(param.substring(param.length() - 40));
    }

    //自定义的大数乘法
    public static String mulBigNum(String param1, String param2) {
        return strToBigDecimal(param1).multiply(strToBigDecimal(param2)).stripTrailingZeros().toPlainString();
    }

    public static String addBigNum(String param1, String param2) {
        return strToBigDecimal(param1).add(strToBigDecimal(param2)).stripTrailingZeros().toPlainString();
    }

    private static BigDecimal strToBigDecimal(String arg) {
        BigDecimal num;
        //判断输入的字符串是否为空
        if (arg == null || "".equals(arg)) {
            //输入的sting为空，转化为0
            num = new BigDecimal("0");
        } else {
            num = new BigDecimal(arg);
        }
        return num;
    }

}
