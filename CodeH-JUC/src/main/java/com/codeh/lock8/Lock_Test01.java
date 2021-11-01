package com.codeh.lock8;

import java.util.concurrent.TimeUnit;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Lock_Test01
 * @date 2021/11/1 14:20
 * @description 八锁问题
 * 1. 标准情况下，两个线程是先打印发短信还是打电话 （1.发短信 2.打电话）
 */
public class Lock_Test01 {
    public static void main(String[] args) {
        Phone1 phone = new Phone1();

        new Thread(() -> {
            phone.sendMsg();
        }, "A").start();

        // 休眠1s
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(() -> {
            phone.call();
        }, "B").start();
    }
}

class Phone1{

    // synchronized锁的对象是方法的调用者
    // 两个方法用的是同一个锁，谁先拿到谁就先执行
    public synchronized void sendMsg() {
        try {
            TimeUnit.SECONDS.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("发短信");
    }

    public synchronized void call() {
        System.out.println("打电话");
    }
}
