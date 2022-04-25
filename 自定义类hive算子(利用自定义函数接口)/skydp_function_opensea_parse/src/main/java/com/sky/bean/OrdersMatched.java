package com.sky.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrdersMatched {
    private String address;
    private String maker;
    private String taker;
    private String buyHash;
    private String sellHash;
    private String price;

    public OrdersMatched() {

    }
}
