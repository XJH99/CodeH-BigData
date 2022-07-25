package com.codeh.bean;

public class Cat {
    private String name = "招财猫";
    public int age = 10;

    public Cat() {}

    public Cat(String name) {
        this.name = name;
    }

    public void hi() {
        //System.out.println("hi " + name);
    }

    public void cry() {
        System.out.println(name + " cry....");
    }
}
