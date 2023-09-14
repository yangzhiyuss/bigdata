package com.decorator_mode;

public class Main {
    public static void main(String[] args) {
        Display b1 = new StringDisplay("Hello, World.");
        Display b2 = new SideBorder(b1, '#');
        Display b3 = new FullBorder(b2);
        b1.show();
        b2.show();
        b3.show();
    }
}
