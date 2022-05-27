package com.skydp.utils;

import org.web3j.utils.Numeric;
import java.math.BigDecimal;

public class ToolUtil {
    //十六进制转化成十进制
    public static String hexToNumStr(String param) {
        return Numeric.toBigInt(param).toString();
    }

    //长度为64的字符串转化为42位的address
    public static String str66To42(String param) {
        return Numeric.prependHexPrefix(param.substring(param.length() - 40));
    }

    public static BigDecimal strToBigDecimal(String arg) {
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
