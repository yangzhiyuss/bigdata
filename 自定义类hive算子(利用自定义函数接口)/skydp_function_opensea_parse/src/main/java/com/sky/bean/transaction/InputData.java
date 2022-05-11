package com.sky.bean.transaction;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class InputData {
    private String[] address;
    private String[] uints;
    private long[] feeMethodsSidesKindsHowToCalls;
    private String calldataBuy;
    private String calldataSell;
    private String replacementPatterBuy;
    private String replacementPatterSell;
    private String staticExtradataBuy;
    private String staticExtradataSell;
    private long[] vs;
    private String[] rssMetadata;
    private int lastIndex;

    public InputData() {
        address = new String[14];
        uints = new String[18];
        feeMethodsSidesKindsHowToCalls = new long[8];
        vs = new long[2];
        rssMetadata = new String[5];
        //-1 表示读取失败
        lastIndex = -1;
    }
}
