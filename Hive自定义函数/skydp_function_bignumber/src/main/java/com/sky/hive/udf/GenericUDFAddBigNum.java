package com.sky.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import java.math.BigDecimal;

@Description(
        name = "add_bignum",
        value = "Returns the add value of string",
        extended = "参数个数大于等于2个,可以输入整数，字符串整数，整数和字符串"
)

public class GenericUDFAddBigNum extends UDF {
    public String evaluate(String[] arguments) throws UDFArgumentLengthException {
        BigDecimal result;

        if (arguments == null || arguments.length < 2) {
            throw new UDFArgumentLengthException("takes less two argument");
        }

        //结果初始化
        result = transferArgToBigDecimal(arguments[0]);
        for (int i = 1; i < arguments.length; i++) {
            result = result.add(transferArgToBigDecimal(arguments[i]));
        }

        return result.stripTrailingZeros().toPlainString();
    }

    private BigDecimal transferArgToBigDecimal(String arg) {
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
