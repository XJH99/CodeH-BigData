package com.codeh.recursion;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className RecursionTest
 * @date 2021/6/16 10:25
 * @description 递归的基本使用
 */
public class RecursionTest {
    public static void main(String[] args) {
        test(4);

        System.out.println(factorial(5));
    }

    public static void test(int n) {
        if (n > 2) {
            test(n - 1);
        } else {
            System.out.println("n=" + n);
        }
    }

    /**
     *
     * @param n
     * @return 阶乘的递归调用
     */
    public static int factorial(int n) {
        if (n == 1) {
            return 1;
        } else {
            return factorial(n - 1) * n;
        }
    }
}
