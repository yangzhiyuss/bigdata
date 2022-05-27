package com.skydp.bean.dws.log;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Erc20Transfer {
    private String address;
    private String symbol;
    private int decimal;
    private String from;
    private String to;
    private String value;

    public Erc20Transfer() {

    }
}
