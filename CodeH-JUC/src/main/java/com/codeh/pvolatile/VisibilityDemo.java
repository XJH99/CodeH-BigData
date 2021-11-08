package com.codeh.pvolatile;

import java.util.concurrent.TimeUnit;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className VisibilityDemo
 * @date 2021/11/8 14:38
 * @description 验证volatile的可见性
 */
public class VisibilityDemo {
    // 不加volatile就会产生死循环
    private volatile static int num = 0;

    public static void main(String[] args) {

        new Thread(() -> {
            while (num == 0) {  // 线程1对主内存的变化是不知道的
                //System.out.println(Thread.currentThread().getName() + "num值改变");
            }
        }).start();

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        num = 1;
        System.out.println(num);

    }
}
