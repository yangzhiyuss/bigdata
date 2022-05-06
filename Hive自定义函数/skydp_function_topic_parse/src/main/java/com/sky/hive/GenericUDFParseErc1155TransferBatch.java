package com.sky.hive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.web3j.utils.Numeric;

@Description(
        name = "parse_erc1155_transferBatch",
        value = "if topics are erc1155 transferSingle return string (format:operator-from-to-ids-values)" +
                "else retun null",
        extended = "参数个数2个，解析erc1155的topic和data"
)

public class GenericUDFParseErc1155TransferBatch extends UDF {
    private final static String ERC1155_TRANSFERBATCH_METHOD =
            "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";

    public String evaluate(String[] arguments) throws UDFArgumentLengthException {
        StringBuilder strBuilder = new StringBuilder();
        if (arguments.length != 2 ) {
            throw new UDFArgumentLengthException("takes only two argument");
        }
        JSONArray jsonArr = JSON.parseArray(arguments[0]);

        //Check whether transfer method is used
        if ( jsonArr == null || jsonArr.size() != 4 || !ERC1155_TRANSFERBATCH_METHOD.equals(jsonArr.getString(0))) {
            return null;
        }

        //get from , to
        String operator = str66To42(jsonArr.getString(1));
        String from = str66To42(jsonArr.getString(2));
        String to = str66To42(jsonArr.getString(3));

        //解析data
        String data = Numeric.cleanHexPrefix(arguments[1]);
        if (data == null || data.length() == 0 ) {
            return null;
        }


        //获取tokenids
        int index = 2 * 64;
        long tokensNum = new Long(strToBigInt(data.substring(index, index + 64)));

        index += 64;
        StringBuilder tokenIds = new StringBuilder();
        for (int i = 0; i < tokensNum; i++) {
            tokenIds.append(strToBigInt(data.substring(index, index + 64))).append(",");
            index += 64;
        }

        if (tokenIds.length() != 0) {
            tokenIds = new StringBuilder(tokenIds.substring(0, tokenIds.length() - 1));
        }

        //获取values
        long valuesCount = new Long(strToBigInt(data.substring(index, index + 64)));
        index += 64;

        StringBuilder values = new StringBuilder();
        for (int i = 0; i < valuesCount; i++) {
            values.append(strToBigInt(data.substring(index, index + 64))).append(",");
            index += 64;
        }

        if (values.length() != 0) {
            values = new StringBuilder(values.substring(0, values.length() - 1));
        }

        //print result
        return strBuilder.append(operator).append("-").append(from)
                .append("-").append(to).append("-").append(tokenIds)
                .append("-").append(values).toString();
    }


    private String strToBigInt(String param) {
        return Numeric.toBigInt(param).toString();
    }


    private String str66To42(String param) {
        return Numeric.prependHexPrefix(param.substring(param.length() - 40));
    }
}
