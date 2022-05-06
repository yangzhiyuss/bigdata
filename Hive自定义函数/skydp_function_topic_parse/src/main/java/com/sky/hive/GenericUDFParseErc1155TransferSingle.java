package com.sky.hive;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.web3j.utils.Numeric;

@Description(
        name = "parse_erc1155_transferSingle",
        value = "if topics are erc1155 transferSingle return string (format:operator-from-to-id-value)" +
                "else retun null",
        extended = "参数个数2个，解析erc1155的topic和data"
)

public class GenericUDFParseErc1155TransferSingle extends UDF {

    private final static String ERC1155_TRANSFERSINGLE_METHOD =
            "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";

    public String evaluate(String[] arguments) throws UDFArgumentLengthException {
        StringBuilder strBuilder = new StringBuilder();
        if (arguments.length != 2 ) {
            throw new UDFArgumentLengthException("takes only two argument");
        }
        JSONArray jsonArr = JSON.parseArray(arguments[0]);

        //Check whether transfer method is used
        if ( jsonArr == null || jsonArr.size() != 4 || !ERC1155_TRANSFERSINGLE_METHOD.equals(jsonArr.getString(0))) {
            return null;
        }

        //get from , to
        String operator = str66To42(jsonArr.getString(1));
        String from = str66To42(jsonArr.getString(2));
        String to = str66To42(jsonArr.getString(3));

        String data = Numeric.cleanHexPrefix(arguments[1]);
        if (data == null) {
            return null;
        }
        //get tokenId value
        String id = "";
        String value = "";
        if (data.length() == 128) {
            id = strToBigInt(data.substring(0, 64));
            value = strToBigInt(data.substring(64));
        }

        //print result
        return strBuilder.append(operator).append("-").append(from)
                .append("-").append(to).append("-").append(id)
                .append("-").append(value).toString();
    }

    private String strToBigInt(String param) {
        return Numeric.toBigInt(param).toString();
    }

    private String str66To42(String param) {
        return Numeric.prependHexPrefix(param.substring(param.length() - 40));
    }

}
