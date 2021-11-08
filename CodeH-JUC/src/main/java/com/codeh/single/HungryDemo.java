package com.codeh.single;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className HungryDemo
 * @date 2021/11/8 15:08
 * @description 单例模式之饿汉式
 */
public class HungryDemo {
    // 私有化构造
    private HungryDemo() {

    }

    private static final HungryDemo instance = new HungryDemo();

    // 可能会浪费资源空间
    public static HungryDemo getInstance() {
        return instance;
    }
}
