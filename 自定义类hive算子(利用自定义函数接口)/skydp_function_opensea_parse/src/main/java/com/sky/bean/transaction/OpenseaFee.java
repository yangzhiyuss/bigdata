package com.sky.bean.transaction;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class OpenseaFee {
    private BigDecimal ethPlatformFeeForSeller;
    private BigDecimal ethPlatformFeesForBuyer;
    private BigDecimal platformFeesPercentForSeller;
    private BigDecimal platformFeesPercentForBuyer;

    public OpenseaFee() {
        this.ethPlatformFeeForSeller = new BigDecimal("0");
        this.ethPlatformFeesForBuyer = new BigDecimal("0");
        this.platformFeesPercentForSeller = new BigDecimal("0");
        this.platformFeesPercentForBuyer = new BigDecimal("0");
    }

    @Override
    public String toString() {
        return ethPlatformFeeForSeller.toString() + "-" + ethPlatformFeesForBuyer.toString() + "-" +
                platformFeesPercentForSeller.toString() + "-" + platformFeesPercentForBuyer.toString();
    }
}
