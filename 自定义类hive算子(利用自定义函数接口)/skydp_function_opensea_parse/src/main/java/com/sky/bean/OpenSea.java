package com.sky.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OpenSea {
    private Transaction transaction;
    private int logIndex;
    private String platform = "";
    private String platformVersion = "";
    private String exchangeContractAddress = "";
    private String ercStandard = "";
    private String seller = "";
    private String buyer = "";
    private String action = "";
    private String originalAmount = "";
    private String originalAmountRaw = "";
    private String originalCurrency = "";
    private String originalCurrencyContract = "";
    private String currencyContract = "";
    private String nftTokenId = "";
    private int nftNum = 0;
    private String nftProjectName = "";
    private String nftContractAddress = "";
    private String ethAmount = "";
    private String usdAmount = "";
    private String tokenPlatformFeesForSeller = "";
    private String ethPlatformFeesForSeller = "";
    private String usdPlatformFeesForSeller = "";
    private String tokenPlatformFeesForBuyer = "";
    private String ethPlatformFeesForBuyer = "";
    private String usdPlatformFeesForBuyer = "";

    public OpenSea(){

    }


    @Override
    public String toString() {
        return transaction.getBlockNumber() + '=' + transaction.getBlockTime() + '=' + transaction.getTxHash() +
                '=' + transaction.getTxFrom() + '=' + transaction.getTxTo() + '=' + transaction.getTxChainId() +
                '=' + logIndex + '=' + platform + '=' + platformVersion + '=' + exchangeContractAddress +
                '=' + ercStandard + '=' + seller + '=' + buyer + '=' + action + '=' + originalAmount +
                '=' + originalAmountRaw + '=' + originalCurrency + '=' + originalCurrencyContract +
                '=' + currencyContract + '=' + nftTokenId + '=' + nftNum + '=' + nftProjectName +
                '=' + nftContractAddress + '=' + ethAmount + '=' + usdAmount +
                '=' + tokenPlatformFeesForSeller + '=' + ethPlatformFeesForSeller +
                '=' + usdPlatformFeesForSeller + '=' + tokenPlatformFeesForBuyer +
                '=' + ethPlatformFeesForBuyer + '=' + usdPlatformFeesForBuyer +
                '=' + transaction.getBlockTime();
    }
}
