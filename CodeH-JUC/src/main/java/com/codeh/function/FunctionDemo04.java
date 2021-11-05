package com.codeh.function;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FunctionDemo04
 * @date 2021/11/5 11:19
 * @description Supplier：供给型接口，没有输入，有返回值
 */
public class FunctionDemo04 {
    public static void main(String[] args) {
//        Supplier<Integer> supplier = new Supplier<Integer>() {
//            @Override
//            public Integer get() {
//                System.out.println("hello");
//                return 1024;
//            }
//        };

        // lambda表达式优化
        Supplier<Integer> supplier = () -> {
            return 1024;
        };

        System.out.println(supplier.get());
    }
}
