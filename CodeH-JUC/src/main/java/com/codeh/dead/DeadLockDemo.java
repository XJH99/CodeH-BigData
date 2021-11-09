package com.codeh.dead;

import java.util.concurrent.TimeUnit;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className DeadLockDemo
 * @date 2021/11/8 18:42
 * @description 死锁：两个线程在互相等待着对方释放锁资源
 */
public class DeadLockDemo {
    private static final String A = "A";
    private static final String B = "B";

    public static void main(String[] args) {
        deadLock();
    }

    public static void deadLock() {
        new Thread(() -> {
            synchronized (A) {
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                synchronized (B) {
                    System.out.println("1");
                }
            }
        }, "A").start();

        new Thread(() -> {
            synchronized (B) {
                synchronized (A) {
                    System.out.println("2");
                }
            }
        }, "B").start();
    }
}
