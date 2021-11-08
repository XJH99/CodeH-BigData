package com.codeh.pvolatile;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className AtomicDemo
 * @date 2021/11/8 14:47
 * @description 验证volatile不保证原子性
 */
public class AtomicDemo {
//    private volatile static int num = 0;
    // 使用原子类可以解决原子性问题
    private volatile static AtomicInteger num = new AtomicInteger();
    public static void main(String[] args) {
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    add();
                }
            }).start();
        }
        // 理论上的结果因该为2000
        while (Thread.activeCount() > 2) {// main gc
            Thread.yield();
        }

        System.out.println(num);
    }

    public static void add() {
        // num++不是一个原子性的操作
//        num++;
        num.getAndIncrement();  // +1操作
    }
}
