package com.codeh.stack;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className InfixToSuffix
 * @date 2021/6/15 16:52
 * @description 中缀表达式转化为后缀表达式
 */
public class InfixToSuffix {
    public static void main(String[] args) {

    }

    /**
     * @param s
     * @return 将中缀表达式转成对应的List
     */
    public static List<String> toInfixExpressionList(String s) {
        List<String> list = new ArrayList<>();

        int i = 0;
        String str;
        char c;

        do {
            // 如果c是一个非数字，加入到list
            if ((c = s.charAt(i)) < 48 || (c = s.charAt(i)) > 57) {
                list.add("" + c);
                i++;
            } else {
                str = "";   // 如果是一个数，需要考虑多位数
                while (i < s.length() && (c = s.charAt(i)) > 48 && (c = s.charAt(i)) <= 57) {
                    str += c;
                    i++;
                }
                list.add(str);
            }
        } while (i < s.length());
        return list;
    }

    /**
     * @param list
     * @return 将得到的中缀表达式list =》 后缀表达式List
     */
    public static List<String> parseSuffixExpressionList(List<String> list) {
        Stack<String> operStack = new Stack<>();

        List<String> list1 = new ArrayList<>(); //用于存放中间结果

        for (String item : list) {
            if (item.matches("\\d+")) {
                list1.add(item);
            } else if (item.equals("(")) {
                operStack.push(item);
            } else if (item.equals(")")) {
                while (!operStack.peek().equals("(")) {
                    list1.add(operStack.pop());
                }

                operStack.pop();
            } else {
                while (operStack.size() != 0 && Operation.getValue(operStack.peek()) >= Operation.getValue(item)) {
                    list1.add(operStack.pop());
                }

                operStack.push(item);
            }
        }

        while (operStack.size() != 0) {
            list1.add(operStack.pop());
        }

        return list1;
    }
}

class Operation {
    private static int ADD = 1;
    private static int SUB = 1;
    private static int MUL = 2;
    private static int DIV = 2;

    public static int getValue(String operation) {
        int result = 0;
        switch (operation) {
            case "+":
                result = ADD;
                break;
            case "-":
                result = SUB;
                break;
            case "*":
                result = MUL;
                break;
            case "/":
                result = DIV;
                break;
            default:
                System.out.println("不存在该运算符");
                break;
        }
        return result;
    }
}
