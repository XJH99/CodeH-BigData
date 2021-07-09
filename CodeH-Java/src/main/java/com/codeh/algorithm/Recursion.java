package com.codeh.algorithm;


/**
 * @author jinhua.xu
 * @version 1.0
 * @className Recursion
 * @date 2021/4/16 10:51
 * @description 递归的使用
 */
public class Recursion {
    public static void main(String[] args) {

        System.out.println("斐波那契数值：\t" + fibonacci(10));
        System.out.println("猴子第5天吃剩下的桃子数：\t" + peach(5));

    }

    /**
     * 斐波那契数：1,1,2,3,5,8,13...
     *
     * @param i 传入的参数
     * @return
     */
    public static int fibonacci(int i) {
        if (i == 1 || i == 2) {
            return 1;
        } else {
            return fibonacci(i - 1) + fibonacci(i - 2);
        }
    }

    /**
     * (i + 1) * 2 +1
     *
     * @param day 传入的天数
     * @return 第一天桃子的个数
     */
    public static int peach(int day) {
        if (day == 10) {
            return 1;
        } else if (day >= 1 && day <= 9) {
            return (peach(day + 1) + 1) * 2;
        } else {
            System.out.println("day 在 0-10之间");
            return -1;
        }
    }
}
