package com.sky.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import java.math.BigDecimal;

@Description(
        name = "dev_bignum",
        value = "Returns the add value of string",
        extended = "参数个数大于等于两个，可以输入整数，字符串整数，整数和字符串"
)

public class GenericUDFDivideBigNum extends UDF {
    public String evaluate(String[] arguments) throws HiveException {
        BigDecimal result;

        if (arguments == null || arguments.length < 2) {
            throw new UDFArgumentLengthException("takes less two argument");
        }

        //结果初始化
        result = transferArgToBigDecimal(arguments[0]);
        BigDecimal tmpData;
        for (int i = 1; i < arguments.length; i++) {
            tmpData = transferArgToBigDecimal(arguments[i]);
            if (tmpData.compareTo(new BigDecimal("0")) == 0) {
                throw new HiveException("zero cannot be divided");
            }
            result = result.divide(tmpData,18, BigDecimal.ROUND_HALF_UP);
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
