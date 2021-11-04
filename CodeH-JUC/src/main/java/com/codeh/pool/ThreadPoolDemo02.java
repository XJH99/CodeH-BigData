package com.codeh.pool;

import java.util.concurrent.*;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ThreadPoolDemo02
 * @date 2021/11/4 10:25
 * @description 阿里巴巴开发手册建议不要使用Executors创建线程池的方式，使用原生的方式进行创建
 * ThreadPoolExecutor.AbortPolicy(): 最大核心线程池 + 阻塞队列满了，还有线程任务进来不处理这个线程的任务，抛出异常
 * ThreadPoolExecutor.CallerRunsPolicy(): 哪来的哪去，主线程过来的主线程执行
 * ThreadPoolExecutor.DiscardPolicy(): 队列满了丢掉任务，不会抛出异常
 * ThreadPoolExecutor.DiscardOldestPolicy()：队列满了，尝试与最早的竞争，也不会抛出异常
 *
 */
public class ThreadPoolDemo02 {
    public static void main(String[] args) {
        ExecutorService threadPoolExecutor = new ThreadPoolExecutor(5,
                Runtime.getRuntime().availableProcessors(),
                5,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(5),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardOldestPolicy());

        // 最大承载数量 = Deque + maximumPoolSize
        try {
            for (int i = 0; i <= 10; i++) {
                threadPoolExecutor.execute(() -> {
                    System.out.println(Thread.currentThread().getName() + "=====>ok");
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            threadPoolExecutor.shutdown();
        }

    }
}
