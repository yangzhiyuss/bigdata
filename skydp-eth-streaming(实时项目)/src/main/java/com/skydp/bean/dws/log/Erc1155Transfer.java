package com.skydp.bean.dws.log;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Erc1155Transfer {
    private String address;
    private String symbol;
    private String from;
    private String to;
    private String tokenId;
    private String value;

    public Erc1155Transfer() {

    }
}
