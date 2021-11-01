package com.codeh.producer_consumer;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className PC_Demo02
 * @date 2021/10/30 16:37
 * @description 使用JUC来实现生产者与消费者的功能
 */
public class PC_Demo02 {
    public static void main(String[] args) {
        Data2 data2 = new Data2();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    data2.increment();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "A").start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    data2.decrement();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "B").start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    data2.increment();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "C").start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    data2.decrement();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "D").start();


    }
}

class Data2 {
    private int number = 0;
    Lock lock = new ReentrantLock();
    // 类似于监听线程的作用
    Condition condition = lock.newCondition();

    /**
     * 生产者：数值加一
     */
    public void increment() throws InterruptedException {
        // 加锁
        lock.lock();
        try {
            // 注意这个地方不能使用if，不然可能会出现虚假唤醒的情况
            while (number != 0) {
                // 等待
                condition.await();
            }
            number++;
            System.out.println(Thread.currentThread().getName() + "==>" + number);
            // 通知其它线程加一完成
            condition.signalAll();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    /**
     * 消费者：数值减一
     */
    public void decrement() throws InterruptedException {
        // 加锁
        lock.lock();
        try {
            // 注意这个地方不能使用if，不然可能会出现虚假唤醒的情况
            while (number == 0) {
                // 等待
                condition.await();
            }
            number--;
            System.out.println(Thread.currentThread().getName() + "==>" + number);
            // 通知其它线程加一完成
            condition.signalAll();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
}
