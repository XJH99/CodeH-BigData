package com.codeh.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ThreadPoolDemo01
 * @date 2021/11/3 18:28
 * @description 创建线程池的方式
 */
public class ThreadPoolDemo01 {
    public static void main(String[] args) {
        ExecutorService threadPool = Executors.newFixedThreadPool(5);   // 创建固定大小的线程池，请求队列长度为Integer.Max_VALUE，可能会堆积大量请求，从而导致OOM
        //ExecutorService threadPool = Executors.newCachedThreadPool();         // 创建可伸缩的线程池，弹性的
        //ExecutorService threadPool = Executors.newSingleThreadExecutor();     // 床架单一线程的线程池
        //ExecutorService threadPool = Executors.newScheduledThreadPool(10); // 周期性的线程池

        try {
            for (int i = 0; i < 10; i++) {
                threadPool.execute(() -> {
                    System.out.println(Thread.currentThread().getName() + "正在执行~~");
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdown();
        }
    }
}
