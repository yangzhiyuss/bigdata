package com.bridge_mode;

public class Main {
    public static void main(String[] args) {
        Display d1 = new Display(new StringDisplayImpl("hello, China."));
        Display d2 = new CountDisplay(new StringDisplayImpl("hello, world."));
        CountDisplay d3 = new CountDisplay(new StringDisplayImpl("hello, universe."));
        d1.display();
        d2.display();
        d3.multiDisplay(5);
    }
}
