package com.codeh.function;

import java.util.function.Function;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FunctionDemo01
 * @date 2021/11/4 11:53
 * @description 函数式接口的使用
 * 一个输入参数T，一个输出参数R
 */
public class FunctionDemo01 {
    public static void main(String[] args) {
//        Function<String, String> function = new Function<String, String>() {
//            @Override
//            public String apply(String o) {
//                return o;
//            }
//        };
        // lambda表达式优化
        Function function = (str) -> {return str;};

        System.out.println(function.apply("hello"));
    }
}
