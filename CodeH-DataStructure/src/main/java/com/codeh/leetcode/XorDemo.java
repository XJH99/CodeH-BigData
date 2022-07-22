package com.codeh.leetcode;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className XorDemo
 * @date 2022/4/18 14:27
 * @description 异或运算使用案例
 */
public class XorDemo {
    public static void main(String[] args) {
        int[] arr = {0, 1, 9, 9, 10, 0, 1, 20};
        //printOdd(arr);
        printOddTimesNum2(arr);

    }

    /**
     * 数组中出现奇数次的数
     * @param arr 原始数组
     */
    public static void printOdd(int[] arr) {
        int eor = 0;
        for (int cur: arr) {
            eor ^= cur;
        }

        System.out.println("出现奇数次的数为：" + eor);
    }

    /**
     * 数组中出现的两个奇数次的数
     * @param arr
     */
    public static void printOddTimesNum2(int[] arr) {
        int eor = 0;
        for (int cur: arr) {
            eor ^= cur;
        }
        // 10010100提取的就是最右边的1：00000100
        // 取出最右边为1的那个数值
        int rightOne = eor & (~eor + 1);
        int num1 = 0;
        for (int cur: arr) {
            if ((rightOne & cur) == 0) {
                num1 ^= cur;
            }
        }

        System.out.printf("两个奇数为num1 = %d, num2 = %d", num1, eor ^ num1);
    }

}
