package com.codeh.stack_and_queue;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className StackConversionQueue
 * @date 2021/8/23 18:53
 * @description 栈与队列之间的转化
 */
public class StackConversionQueue {
    public static void main(String[] args) {


    }
}

// 两个栈实现队列功能：最坏的时间复杂度为O(n)
class StackToQueue {
    private Stack<Integer> stack1;
    private Stack<Integer> stack2;

    public StackToQueue() {
        this.stack1 = new Stack<>();
        this.stack2 = new Stack<>();
    }

    /**
     * stack1的入栈操作
     *
     * @param n
     */
    public void push(int n) {
        stack1.push(n);
    }

    /**
     * 返回队头元素
     *
     * @return
     */
    public int peekData() {
        if (stack2.isEmpty())
            while (!stack1.isEmpty()) {
                stack2.push(stack1.pop());
            }
        return stack2.peek();
    }

    /**
     * stack2出栈操作
     *
     * @return
     */
    public int pop() {
        peekData();
        return stack2.pop();
    }

    /**
     * 两个栈都为空，说明队列为空
     *
     * @return
     */
    public Boolean empty() {
        return stack1.isEmpty() && stack2.isEmpty();
    }
}


// 队列实现栈的功能
class QueueToStack {
    Queue<Integer> queue = new LinkedList<>();

    // 栈顶元素
    int top = 0;

    /**
     * 添加元素到栈顶
     *
     * @param n
     */
    public void push(int n) {
        queue.offer(n);
        // 将新加入的元素保存到top临时变量中
        top = n;
    }

    /**
     * 返回栈顶元素
     *
     * @return
     */
    public int getTop() {
        return top;
    }

    /**
     * 出栈操作
     */
    public int pop() {
        int size = queue.size();

        // 留下队尾两个元素
        while (size > 2) {
            queue.offer(queue.poll());
            size--;
        }

        // 保留新的队尾元素
        top = queue.peek();

        queue.offer(queue.poll());

        // 删除队尾元素
        return queue.poll();
    }
}