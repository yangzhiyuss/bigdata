package com.sky.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Transaction {
    private String blockNumber = "";
    private String blockTime = "";
    private String txHash = "";
    private String txFrom = "";
    private String txTo = "";
    private String txChainId = "";

    public Transaction() {

    }
}
