package com.skydp.bean.dws.transaction;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Transaction {
    private long blockNumber;
    private String blockTime = "";
    private String txHash = "";
    private String txFrom = "";
    private String txTo = "";
    private int txChainId;

    public Transaction() {

    }
}
