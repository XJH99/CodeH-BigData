package com.codeh.test;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Test
 * @date 2021/4/15 15:15
 * @description TODO
 */
public class Test {

    @org.junit.Test
    public void test1() {
        int i = 1;
        i = i++;

        System.out.println(i);

        System.out.println((4<1) ^ (2>1));
    }
}
