package com.sky.bean.log;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Erc721Transfer {
    private String address;
    private String symbol;
    private String from;
    private String to;
    private String tokenId;

    public Erc721Transfer() {

    }
}
