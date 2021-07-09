package com.codeh.stack;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Calculator
 * @date 2021/6/15 10:34
 * @description 栈实现综合计算器（中缀表达式）
 * <p>
 * 思路：
 * 1.两个栈，一个数栈，一个符号栈
 * 2.如果符号栈为空，直接入符号栈；数字就直接入数栈
 * 3.如果符号栈有操作符，就进行比较，如果当前的操作符的优先级小于或等于栈中的操作符；那么就从数栈中pop出两个数，在符号栈中pop一个符号进行运算；
 * 将得到的结果入数栈，然后将当前的操作符入符号栈；如果入的符号优先级大于栈中的操作符，直接入符号栈
 */
public class Calculator {
    public static void main(String[] args) {
        String expression = "7*2*2-5+1-5+3/3"; // 20

        // 创建两个栈
        ArrayStack2 numStack = new ArrayStack2(10);
        ArrayStack2 operStack = new ArrayStack2(10);

        // 定义变量
        int index = 0;
        char ch = ' ';
        String keepNum = "";
        int num1 = 0;
        int num2 = 0;
        int oper = 0;
        int res = 0;

        while (true) {
            ch = expression.substring(index, index + 1).charAt(0);

            // 对字符进行判断
            if (operStack.isOperator(ch)) {
                // 字符栈为空，直接加入
                if (operStack.isEmpty()) {
                    operStack.push(ch);
                } else {
                    // 字符栈不为空,传入的运算符优先级小于栈中的优先级
                    if (operStack.priority(ch) <= operStack.priority(operStack.peek())) {
                        num1 = numStack.pop();
                        num2 = numStack.pop();
                        oper = operStack.pop();
                        res = numStack.cal(num1, num2, oper);

                        // 运算结果再次入栈
                        numStack.push(res);
                        operStack.push(ch);
                    } else {
                        operStack.push(ch);
                    }
                }
            } else {
                // 为数字，判断是一位数还是多位数
                keepNum += ch;

                // 如果ch已经是最后一位，直接入栈
                if (index == expression.length() - 1) {
                    numStack.push(Integer.parseInt(keepNum));
                } else {
                    // 判断下一个字符是不是数字
                    if (operStack.isOperator(expression.substring(index + 1, index + 2).charAt(0))) {
                        // 为操作符，直接入数栈
                        numStack.push(Integer.parseInt(keepNum));
                        keepNum = "";
                    }
                }

            }

            index++;
            if (index >= expression.length()) {
                break;
            }

        }


        // 扫描完毕
        while (true) {
            if (operStack.isEmpty()) {
                break;
            }

            num1 = numStack.pop();
            num2 = numStack.pop();
            oper = operStack.pop();
            res = numStack.cal(num1, num2, oper);

            numStack.push(res);
        }

        int res2 = numStack.pop();
        System.out.printf("表达式 %s=%d", expression, res2);

    }

}

// 定义一个栈，实现前面栈的扩展功能
class ArrayStack2 {
    private int maxSize;
    private int[] stack;
    private int top = -1;

    public ArrayStack2(int maxSize) {
        this.maxSize = maxSize;
        stack = new int[this.maxSize];
    }

    /**
     * @return 返回栈顶元素
     */
    public int peek() {
        if (isEmpty()) {
            throw new RuntimeException("栈为空~");
        }

        return stack[top];
    }

    /**
     * @return 栈满
     */
    public Boolean isFull() {
        return top == maxSize - 1;
    }

    /**
     * @return 栈空
     */
    public Boolean isEmpty() {
        return top == -1;
    }

    /**
     * @param value 入栈操作
     */
    public void push(int value) {
        if (isFull()) {
            System.out.println("栈已满~");
            return;
        }

        top++;
        stack[top] = value;
    }

    /**
     * @return 出栈操作
     */
    public int pop() {
        if (isEmpty()) {
            throw new RuntimeException("栈为空~");
        }

        int value = stack[top];
        top--;
        return value;
    }

    /**
     * 遍历栈元素
     */
    public void list() {
        if (isEmpty()) {
            System.out.println("栈空，没有元素~~");
            return;
        }

        for (int i = top; i >= 0; i--) {
            System.out.printf("stack[%d] = %d\n", i, stack[i]);
        }
    }

    /**
     * @param operator 返回运算符的优先级
     * @return
     */
    public int priority(int operator) {
        if (operator == '*' || operator == '/') {
            return 1;
        } else if (operator == '+' || operator == '-') {
            return 0;
        } else {
            return -1; // 假定运算符只有加减乘除
        }
    }

    /**
     * @param val 传入的运算符
     * @return 判断是不是一个运算符
     */
    public Boolean isOperator(char val) {
        return val == '+' || val == '-' || val == '*' || val == '/';
    }

    /**
     * @param num1
     * @param num2
     * @param operator
     * @return 计算方法
     */
    public int cal(int num1, int num2, int operator) {
        int res = 0;
        switch (operator) {
            case '+':
                res = num1 + num2;
                break;
            case '-':
                res = num2 - num1;
                break;
            case '*':
                res = num1 * num2;
                break;
            case '/':
                res = num2 / num1;
                break;
            default:
                break;
        }

        return res;
    }

}
