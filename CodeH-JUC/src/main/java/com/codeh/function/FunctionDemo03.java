package com.codeh.function;

import java.util.function.Consumer;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FunctionDemo03
 * @date 2021/11/5 11:13
 * @description 消费型接口Consumer：只有输入，没有返回值
 */
public class FunctionDemo03 {
    public static void main(String[] args) {
//        Consumer<String> consumer = new Consumer<String>() {
//            @Override
//            public void accept(String str) {
//                System.out.println(str);
//            }
//        };
        // lambda表达式优化
        Consumer<String> consumer = (String str) -> {
            System.out.println(str);
        };

        consumer.accept("consumer");
    }
}
