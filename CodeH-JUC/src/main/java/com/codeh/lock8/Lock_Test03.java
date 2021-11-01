package com.codeh.lock8;

import java.util.concurrent.TimeUnit;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Lock_Test03
 * @date 2021/11/1 14:20
 * @description 八锁问题
 * 1.加上static后，锁的就是全局唯一的class
 * 2.一个静态同步方法，一个普通同步方法（前者锁的是全局class，后者锁的是当前调用的对象）
 */
public class Lock_Test03 {
    public static void main(String[] args) {
        // 两个对象，两个方法
        Phone3 phone1 = new Phone3();
        Phone3 phone2 = new Phone3();

        new Thread(() -> {
            phone1.sendMsg();
        }, "A").start();

        // 休眠1s
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(() -> {
            phone2.call();
        }, "B").start();
    }
}

class Phone3{

    // synchronized锁的对象是方法的调用者
    // 两个方法用的是同一个锁，谁先拿到谁就先执行
    // static静态方法，类一加载就有了，class全局唯一，锁的是class
    public static synchronized void sendMsg() {
        try {
            TimeUnit.SECONDS.sleep(4);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("发短信");
    }

    // 普通同步方法
    public  synchronized void call() {
        System.out.println("打电话");
    }

    // 这里没有加锁，不受锁的影响
    public void hello() {
        System.out.println("hello");
    }
}
