package com.yzy.chain_resposible_mode;

public abstract class Handler {
    protected Handler next;

    public void setNext(Handler next){
        this.next = next;
    }

    public abstract void handle(String request);
}
