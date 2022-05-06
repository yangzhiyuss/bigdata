import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.web3j.utils.Numeric;

import java.nio.charset.StandardCharsets;

public class Test {
    public static String data = "0x0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000100000000000000000000000000000a2400000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000001";

    public static void main(String[] args) {

        String data = Numeric.cleanHexPrefix("0x0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        if (data == null || data.length() == 0 ) {
            return;
        }

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


        long valuesCount = new Long(strToBigInt(data.substring(index, index + 64)));
        index += 64;

        StringBuilder values = new StringBuilder();
        for (int i = 0; i < valuesCount; i++) {
            values.append(strToBigInt(data.substring(index, index + 64))).append(",");
            index += 64;
        }

        if (values.length() != 0) {
            tokenIds = new StringBuilder(tokenIds.substring(0, tokenIds.length() - 1));
        }

        System.out.println(tokenIds);
        System.out.println(values);
    }

    private static String strToBigInt(String param) {
        return Numeric.toBigInt(param).toString();
    }

    private String str66To42(String param) {
        return Numeric.prependHexPrefix(param.substring(param.length() - 40));
    }
}
