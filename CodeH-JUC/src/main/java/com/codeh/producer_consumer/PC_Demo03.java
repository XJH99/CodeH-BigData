package com.codeh.producer_consumer;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className PC_Demo03
 * @date 2021/11/1 10:16
 * @description 通过lock中的condition可以实现线程之间的精准通知唤醒
 */
public class PC_Demo03 {
    public static void main(String[] args) {
        Data3 data3 = new Data3();
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                data3.A();
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                data3.B();
            }
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                data3.C();
            }
        }).start();

    }
}


class Data3 {
    private int number = 1;
    Lock lock = new ReentrantLock();
    // 创建三个监听器
    Condition condition1 = lock.newCondition();
    Condition condition2 = lock.newCondition();
    Condition condition3 = lock.newCondition();

    public void A() {
        lock.lock();
        try {
            // 实际业务
            while (number != 1) {
                condition1.await();
            }
            // 精准唤醒指定的线程
            number = 2;
            System.out.println(Thread.currentThread().getName() + "====> AAAAAAA");
            condition2.signal();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void B() {
        lock.lock();
        try {
            // 实际业务
            while (number != 2) {
                condition2.await();
            }
            // 精准唤醒指定的线程
            number = 3;
            System.out.println(Thread.currentThread().getName() + "====> BBBBBBB");
            condition3.signal();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void C() {
        lock.lock();
        try {
            // 实际业务
            while (number != 3) {
                condition3.await();
            }
            // 精准唤醒指定的线程
            number = 1;
            System.out.println(Thread.currentThread().getName() + "====> CCCCCCC");
            condition1.signal();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
