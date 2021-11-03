package com.codeh.blockqueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SynchronizedQueueDemo
 * @date 2021/11/3 16:33
 * @description 同步队列，不存储元素，一个线程向同步队列中添加了一个元素就要取出才能添加
 */
public class SynchronizedQueueDemo {
    public static void main(String[] args) {
        BlockingQueue<String> blockingQueue = new SynchronousQueue<>();


        new Thread(() -> {
            try {

                System.out.println(Thread.currentThread().getName() + "添加了元素put(a)");
                blockingQueue.put("a");

                System.out.println(Thread.currentThread().getName() + "添加了元素put(b)");
                blockingQueue.put("b");

                System.out.println(Thread.currentThread().getName() + "添加了元素put(c)");
                blockingQueue.put("c");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "T1").start();

        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                System.out.println(Thread.currentThread().getName() + "移除元素" + blockingQueue.take());
                TimeUnit.SECONDS.sleep(2);
                System.out.println(Thread.currentThread().getName() + "移除元素" + blockingQueue.take());
                TimeUnit.SECONDS.sleep(2);
                System.out.println(Thread.currentThread().getName() + "移除元素" + blockingQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, "T2").start();

    }
}
