package com.codeh.single;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className InnerDemo
 * @date 2021/11/8 15:20
 * @description 使用内部类创建单例
 */
public class InnerDemo {
    private InnerDemo() {

    }

    public static InnerDemo getInstance() {
        return InnerClass.instance;
    }

    public static class InnerClass{
        private static final InnerDemo instance = new InnerDemo();
    }
}
