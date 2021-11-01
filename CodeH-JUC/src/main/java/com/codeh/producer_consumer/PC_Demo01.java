package com.codeh.producer_consumer;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className PC_Demo01
 * @date 2021/10/30 16:15
 * @description 基础版本的生产者消费者代码：
 * 线程之间的通信问题：生产者和消费者问题，等待唤醒，通知唤醒
 * 线程之间交替执行 A, B操作同一个变量 number = 0
 */
public class PC_Demo01 {
    public static void main(String[] args) {
        Data data = new Data();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    data.increment();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "A").start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    data.decrement();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "B").start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    data.increment();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "C").start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    data.decrement();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "D").start();

    }
}

//todo 判断业务，等待，通知
class Data {
    private int number = 0;

    /**
     * 生产者：数值加一
     */
    public synchronized void increment() throws InterruptedException {
        // 注意这个地方不能使用if，不然可能会出现虚假唤醒的情况
        while (number != 0) {
            this.wait();
        }
        number++;
        System.out.println(Thread.currentThread().getName() + "==>" + number);
        // 通知其它线程加一完成
        this.notifyAll();
    }

    /**
     * 消费者：数值减一
     */
    public synchronized void decrement() throws InterruptedException {
        // 注意这个地方不能使用if，不然可能会出现虚假唤醒的情况
        while (number == 0) {
            this.wait();
        }
        number--;
        System.out.println(Thread.currentThread().getName() + "==>" + number);
        // 通知其它线程减一完成
        this.notifyAll();
    }
}
