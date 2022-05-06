package com.sky.hive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.web3j.utils.Numeric;

import java.nio.charset.StandardCharsets;

@Description(
        name = "parse_erc721_transfer",
        value = "if topics are erc721 transfer return string (format:from-to-tokenId)" +
                "else retun null",
        extended = "参数个数1个，解析erc721的topic"
)

public class GenericUDFParseErc721Transfer extends UDF {

    private final static String ERC721_TRANSFER_METHOD =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    public String evaluate(String[] arguments) throws UDFArgumentLengthException {
        StringBuilder strBuilder = new StringBuilder();
        if (arguments.length != 1 ) {
            throw new UDFArgumentLengthException("takes only one argument");
        }
        JSONArray jsonArr = JSON.parseArray(arguments[0]);

        //Check whether transfer method is used
        if ( jsonArr == null || jsonArr.size() != 4 || !ERC721_TRANSFER_METHOD.equals(jsonArr.getString(0))) {
            return null;
        }

        //get from , to , tokendi
        String from = str66To42(jsonArr.getString(1));
        String to = str66To42(jsonArr.getString(2));
        String tokenId = strToBigInt(jsonArr.getString(3));

        //print result
        return strBuilder.append(from).append("-").append(to).append("-").append(tokenId).toString();
    }

    private String strToBigInt(String param) {
        return Numeric.toBigInt(param).toString();
    }

    private String str66To42(String param) {
        return Numeric.prependHexPrefix(param.substring(param.length() - 40));
    }
}
