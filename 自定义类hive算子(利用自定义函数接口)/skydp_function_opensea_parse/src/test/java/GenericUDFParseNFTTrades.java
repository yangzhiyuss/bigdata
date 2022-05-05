import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.sky.util.ToolUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;

@Description(
        name = "parse_nft_trades",
        value = "if topics are erc20 transfer return string (format:from-to-value)" +
                "else retun null",
        extended = "参数个数2个，解析erc20的topic和data"
)

public class GenericUDFParseNFTTrades extends UDF {
    public String evaluate(String[] arguments) throws UDFArgumentLengthException {
        StringBuilder strBuilder = new StringBuilder();
        if (arguments == null || arguments.length != 1) {
            return null;
        }

        String[] param = new String[3];

        JSONArray jsonArr = JSON.parseArray(arguments[0]);
        String method = "";

        //Check whether transfer method is used
        if ( jsonArr == null || jsonArr.size() != 3 || !method.equals(jsonArr.getString(0))) {
            return null;
        }
        param[0] = ToolUtil.str66To42(jsonArr.getString(1));
        param[1] = ToolUtil.str66To42(jsonArr.getString(2));

        //The second argument cannot be empty
        if (arguments[1] == null || "".equals(arguments[1])) {
            return null;
        }
        param[2] = ToolUtil.hexToNumStr(arguments[1]);

        return strBuilder.append(param[0]).append("-").append(param[1]).append("-").append(param[2]).toString();
    }

}
