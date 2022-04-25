package com.sky.hive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.web3j.utils.Numeric;
import java.nio.charset.StandardCharsets;

@Description(
        name = "parse_erc20_transfer",
        value = "if topics are erc20 transfer return string (format:from-to-value)" +
                "else retun null",
        extended = "参数个数2个，解析erc20的topic和data"
)

public class GenericUDFParseErc20Transfer extends UDF {
    public String evaluate(String[] arguments) throws UDFArgumentLengthException {
        StringBuilder strBuilder = new StringBuilder();
        String[] param = new String[3];
        if (arguments.length != 2 ) {
            throw new UDFArgumentLengthException("takes only two argument");
        }
        JSONArray jsonArr = JSON.parseArray(arguments[0]);
        String method = methodSha3Str();

        //Check whether transfer method is used
        if ( jsonArr == null || jsonArr.size() != 3 || !method.equals(jsonArr.getString(0))) {
            return null;
        }
        param[0] = str66To42(jsonArr.getString(1));
        param[1] = str66To42(jsonArr.getString(2));

        //The second argument cannot be empty
        if (arguments[1] == null || "".equals(arguments[1])) {
            return null;
        }
        param[2] = strToBigInt(arguments[1]);

        return strBuilder.append(param[0]).append("-").append(param[1]).append("-").append(param[2]).toString();
    }

    private String methodSha3Str() {
        byte[] bytes = "Transfer(address,address,uint256)".getBytes(StandardCharsets.UTF_8);
        Keccak.Digest256 kecc = new Keccak.Digest256();
        kecc.update(bytes);
        return Numeric.toHexString(kecc.digest());
    }

    private String strToBigInt(String param) {
        return Numeric.toBigInt(param).toString();
    }

    private String str66To42(String param) {
        return Numeric.prependHexPrefix(param.substring(param.length() - 40));
    }
}
