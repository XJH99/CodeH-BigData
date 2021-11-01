package com.codeh.lock8;

import java.util.concurrent.TimeUnit;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Lock_Test02
 * @date 2021/11/1 14:20
 * @description 八锁问题
 * 1.增加一个普通方法后，会先执行普通方法，因为普通方法不受锁的影响
 * 2.两个对象，两个同步方法，因为锁的是方法的调用者，所以两个锁互不影响
 */
public class Lock_Test02 {
    public static void main(String[] args) {
        Phone2 phone1 = new Phone2();
        Phone2 phone2 = new Phone2();

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

class Phone2{

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

    // 这里没有加锁，不受锁的影响
    public void hello() {
        System.out.println("hello");
    }
}
