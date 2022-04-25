package com.sky.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Log {
    private String address;
    private Object[] topics;
    private String data;
    private String ercType;
    private String symBol;
    private int decimal;
    private int logIndex;

    public Log() {

    }
}
