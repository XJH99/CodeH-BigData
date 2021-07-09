package com.codeh.stack;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className PolandNotation
 * @date 2021/6/15 16:10
 * @description 逆波兰计算器：使用后缀表达式进行计算
 */
public class PolandNotation {
    public static void main(String[] args) {

        // 4*5-8+60+8/2
        String suffixExpression = "4 5 * 8 - 60 + 8 2 / +";

        List<String> list = getListString(suffixExpression);
        System.out.println("list=" + list);

        int calculator = calculator(list);
        System.out.println("表达式计算的结果为：" + calculator);


    }

    /**
     * @param suffixExpression
     * @return 将表达式进行切割，存放到一个List当中
     */
    public static List<String> getListString(String suffixExpression) {
        String[] split = suffixExpression.split(" ");

        ArrayList<String> list = new ArrayList<>();

        for (String s : split) {
            list.add(s);
        }

        return list;
    }

    /**
     *
     * @param list
     * @return 传入一个list，进行计算
     */
    public static int calculator(List<String> list) {
        Stack<String> stack = new Stack<>();

        for (String item: list) {
            if (item.matches("\\d+")) { // 正则表达式匹配多位数
                stack.push(item);
            } else {
                // 取出两个数，再运算结果入栈
                int num1 = Integer.parseInt(stack.pop());
                int num2 = Integer.parseInt(stack.pop());
                int res = 0;

                if (item.equals("+")) {
                    res = num1 + num2;
                } else if (item.equals("-")) {
                    res = num2 -num1;
                } else if (item.equals("*")) {
                    res = num1 * num2;
                } else if (item.equals("/")) {
                    res = num2 / num1;
                } else {
                    throw new RuntimeException("运算符有误");
                }

                stack.push("" + res);
            }
        }
        // 返回最后栈中的结果
        return Integer.parseInt(stack.pop());
    }

}
