package com.codeh.function;

import java.util.function.Predicate;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FunctionDemo02
 * @date 2021/11/4 12:01
 * @description 断定式接口
 * 传入一个参数T，返回一个Boolean类型的值
 */
public class FunctionDemo02 {
    public static void main(String[] args) {
//        Predicate<String> predicate = new Predicate<String>() {
//            @Override
//            public boolean test(String str) {
//                return str.isEmpty();
//            }
//        };
        // lambda表达式优化
        Predicate<String> predicate = (str) -> {return str.isEmpty();};

        System.out.println(predicate.test(""));
    }
}
