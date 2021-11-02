package com.codeh.assist;

import java.util.concurrent.CountDownLatch;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className CountDownLatchTest
 * @date 2021/11/2 16:46
 * @description 辅助类之CountDownLatch使用：用于计数线程是否执行完成，完成-1
 */
public class CountDownLatchTest {
    public static void main(String[] args) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(10);

        for (int i = 1; i <= 10; i++) {
            new Thread(() -> {
                System.out.println(Thread.currentThread().getName() + "执行完成");
                countDownLatch.countDown(); // 数量-1
            }, String.valueOf(i)).start();
        }

        countDownLatch.await(); // 等待计数器归零，然后向下执行

        System.out.println("close door");

    }
}
