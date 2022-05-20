import com.sky.func.GenericUDTFParseNFTTrades;
import com.sky.func.ParseInputDataOfOpensea;
import com.sky.util.ToolUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.velocity.runtime.directive.Parse;

import java.math.BigDecimal;

public class Test {
    //汇率除数
    private final static BigDecimal INVERSE_BASIS_POINT = new BigDecimal(10000);

    public static void main(String[] args) throws HiveException {
        System.out.println(
                new BigDecimal("1000000.0000000000000000000000").stripTrailingZeros().toPlainString());
    }
}
