package com.codeh.bean;

public class Car {
    public String name = "汽车";
    public String brand = "宝马";
    public Double price = 500000d;

    @Override
    public String toString() {
        return "Car{" +
                "name='" + name + '\'' +
                ", brand='" + brand + '\'' +
                ", price=" + price +
                '}';
    }
}
