package com.codeh.queue;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className QueueDemo
 * @date 2021/6/2 17:48
 * @description 数组模拟队列功能
 */
public class QueueDemo {

}

/**
 * 基础模拟队列功能
 */
class ArrayQueue{
    private int maxSize; // 最大队列容量
    private int front;
    private int rear;
    private int[] arr;

    /**
     * 构造器
     *
     * @param size
     */
    public ArrayQueue(int size) {
        maxSize = size;
        arr = new int[maxSize];
        front = -1;
        rear = -1;
    }

    /**
     * 判断队列满
     *
     * @return
     */
    public Boolean full() {
        return rear == maxSize - 1;
    }

    /**
     * 判断队列为空
     *
     * @return
     */
    public Boolean isEmpty() {
        return front == rear;
    }

    /**
     * 入队
     *
     * @param n
     */
    public void addQueue(int n) {
        if (full()) {
            System.out.println("队列已满~~");
            return;
        }

        rear++;    // rear后移
        arr[rear] = n;
    }

    /**
     * 出队
     *
     * @return
     */
    public int removeQueue() {
        if (isEmpty()) {
            throw new RuntimeException("当前队列中没有元素~~");
        }

        front++;
        return arr[front];
    }

    /**
     * 显示队列所有数据
     */
    public void showQueue() {
        for (int i = 0; i < arr.length; i++) {
            System.out.printf("arr[%d] = %d\n", i, arr[i]);
        }
    }

    /**
     * 显示头元素
     *
     * @return
     */
    public int head() {
        if (isEmpty()) {
            throw new RuntimeException("当前队列为空");
        }

        return arr[front + 1];
    }

}


/**
 * 环形队列功能实现
 */
class CycleQueue{
    private int maxSize;
    private int front;
    private int rear;
    private int[] arr;

    public CycleQueue(int size) {
        maxSize = size;
        arr = new int[maxSize];
    }

    public Boolean isFull() {
        return (rear + 1) % maxSize == front;
    }

    public Boolean isEmpty() {
        return rear == front;
    }

    /**
     * 出队
     *
     * @return
     */
    public int getQueue() {
        if (isEmpty()) {
            throw new RuntimeException("队列为空");
        }

        int tmp = arr[front];
        front = (front + 1) % maxSize;
        return tmp;
    }

    /**
     * 入队
     *
     * @param n
     */
    public void addQueue(int n) {
        if (isFull()) {
            System.out.println("队列已满");
            return;
        }
        arr[rear] = n;
        rear = (rear + 1) % maxSize;
    }

    /**
     * 有效元素个数
     *
     * @return
     */
    public int size() {
        return (rear + maxSize - front) % maxSize;
    }

    /**
     * 显示头元素
     *
     * @return
     */
    public int head() {
        if (isEmpty()) {
            throw new RuntimeException("队列为空");
        }

        return arr[front];
    }

    public void show() {
        if (isEmpty()) {
            throw new RuntimeException("队列为空");
        }

        for (int i = front; i < front + size(); i++) {
            System.out.printf("arr[%d]=%d\n", i % maxSize, arr[i % maxSize]);
        }
    }
}