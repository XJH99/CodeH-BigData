package com.codeh.stack;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ArrayStackDemo
 * @date 2021/6/8 10:05
 * @description 数组模拟栈功能
 */
public class ArrayStackDemo {
    public static void main(String[] args) {
        ArrayStack stack = new ArrayStack(3);

        stack.push(1);
        stack.push(2);
        stack.push(3);

        stack.list();

        stack.pop();
        stack.pop();

        stack.list();

    }
}


class ArrayStack {
    private int maxSize;
    private int[] stack;
    private int top = -1;  // 表示栈顶，初始化为-1

    public ArrayStack(int size) {
        this.maxSize = size;
        this.stack = new int[maxSize];
    }

    /**
     * @return 返回栈满的判断情况
     */
    public boolean isFull() {
        return top == maxSize - 1;
    }

    /**
     * @return 栈空的情况
     */
    public boolean isEmpty() {
        return top == -1;
    }

    /**
     * @param value 入栈插入元素
     */
    public void push(int value) {
        if (isFull()) {
            System.out.println("栈已满，请不要插入数据~~");
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
            throw new RuntimeException("栈空，没有数据~~");
        }

        int value = stack[top];
        top--;
        return value;
    }

    /**
     * 从栈顶开始进行出栈操作
     */
    public void list() {
        if (isEmpty()) {
            System.out.println("栈中没有元素~~");
            return;
        }

        for (int i = top; i >= 0; i--) {
            System.out.printf("stack[%d]=%d\n", i, stack[i]);
        }
    }
}
