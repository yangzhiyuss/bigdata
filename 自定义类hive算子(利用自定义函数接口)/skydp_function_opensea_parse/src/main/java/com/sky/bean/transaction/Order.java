package com.sky.bean.transaction;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class Order {
    /* Exchange address, intended as a versioning mechanism. */
    private String exchange;
    /* Order maker address. */
    private String maker;
    /* Order taker address, if specified. */
    private String taker;
    /* Maker relayer fee of the order, unused for taker order. */
    private BigDecimal makerRelayerFee;
    /* Taker relayer fee of the order, or maximum taker fee for a taker order. */
    private BigDecimal takerRelayerFee;
    /* Maker protocol fee of the order, unused for taker order. */
    private BigDecimal makerProtocolFee;
    /* Taker protocol fee of the order, or maximum taker fee for a taker order. */
    private BigDecimal takerProtocolFee;
    /* Order fee recipient or zero address for taker order. */
    private String feeRecipient;
    /* Fee method (protocol token or split fee). */
    private FeeMethod feeMethod;
    /* Side (buy/sell). */
    private Side side;
    /* Kind of sale. */
    private SaleKind saleKind;
    /* Target. */
    private String target;
    /* HowToCall. */
    private HowToCall howToCall;
    /* Calldata. */
    private String call_data;
    /* Calldata replacement pattern, or an empty byte array for no replacement. */
    private String replacementPattern;
    /* Static call target, zero-address for no static call. */
    private String staticTarget;
    /* Static call extra data. */
    private String staticExtradata;
    /* Token used to pay for the order, or the zero-address as a sentinel value for Ether. */
    private String paymentToken;
    /* Base price of the order (in paymentTokens). */
    private String basePrice;
    /* Auction extra parameter - minimum bid increment for English auctions, starting/ending price difference. */
    private String extra;
    /* Listing timestamp. */
    private String listingTime;
    /* Expiration timestamp - 0 for no expiry. */
    private String expirationTime;
    /* Order salt, used to prevent duplicate hashes. */
    private String salt;

}

