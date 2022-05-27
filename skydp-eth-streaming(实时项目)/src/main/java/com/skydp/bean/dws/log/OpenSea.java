package com.skydp.bean.dws.log;


import com.skydp.bean.dws.transaction.Transaction;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;

@Data
@AllArgsConstructor
public class OpenSea {
    private long blockNumber;
    private String blockTime = "";
    private String txHash = "";
    private String txFrom = "";
    private String txTo = "";
    private int txChainId;
    private int logIndex;
    private String platform = "";
    private String platformVersion = "";
    private String exchangeContractAddress = "";
    private String ercStandard = "";
    private String seller = "";
    private String buyer = "";
    private String action = "";
    private BigDecimal originalAmount;
    private BigInteger originalAmountRaw;
    private String originalCurrency = "";
    private String originalCurrencyContract = "";
    private String currencyContract = "";
    private String nftTokenId = "";
    private int nftNum;
    private String nftProjectName = "";
    private String nftContractAddress = "";
    private BigDecimal ethAmount;
    private BigDecimal usdAmount;
    private BigDecimal tokenPlatformFeesForSeller;
    private BigDecimal ethPlatformFeesForSeller;
    private BigDecimal usdPlatformFeesForSeller;
    private BigDecimal tokenPlatformFeesForBuyer;
    private BigDecimal ethPlatformFeesForBuyer;
    private BigDecimal usdPlatformFeesForBuyer;
    private String dt;

    public OpenSea(){

    }

    public void setTransaction(Transaction transaction) {
        this.blockNumber = transaction.getBlockNumber();
        this.blockTime = transaction.getBlockTime();
        this.txHash = transaction.getTxHash();
        this.txFrom = transaction.getTxFrom();
        this.txTo = transaction.getTxTo();
        this.txChainId = transaction.getTxChainId();
    }

    @Override
    public String toString() {
        return blockNumber + '=' + blockTime + '=' + txHash + '=' + txFrom + '=' + txTo + '=' + txChainId +
                '=' + logIndex + '=' + platform + '=' + platformVersion + '=' + exchangeContractAddress +
                '=' + ercStandard + '=' + seller + '=' + buyer + '=' + action + '=' + originalAmount +
                '=' + originalAmountRaw + '=' + originalCurrency + '=' + originalCurrencyContract +
                '=' + currencyContract + '=' + nftTokenId + '=' + nftNum + '=' + nftProjectName +
                '=' + nftContractAddress + '=' + ethAmount + '=' + usdAmount +
                '=' + tokenPlatformFeesForSeller + '=' + ethPlatformFeesForSeller +
                '=' + usdPlatformFeesForSeller + '=' + tokenPlatformFeesForBuyer +
                '=' + ethPlatformFeesForBuyer + '=' + usdPlatformFeesForBuyer +
                '=' + dt;
    }
}
