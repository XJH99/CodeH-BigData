package com.codeh.thread;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Application
 * @date 2021/12/13 9:44
 * @description TODO
 */
public class Application {
    private static final Integer MAX_RETRIES = 3;
    private static ExecutorService executorService = Executors.newFixedThreadPool(10);
    private static Object lock = new Object();
    private static volatile int count = 0;

    public static void main(String[] args) throws Exception {
        HashMap<TaskMetaData, Future<Integer>> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            TaskMetaData taskMetaData = new TaskMetaData(i, "task-" + i, 0);
            map.put(taskMetaData, submit(0));
        }

        boolean flag = true;
        while (flag) {
            ArrayList<TaskMetaData> keys = new ArrayList<>();
            for (TaskMetaData taskMetaData : map.keySet()) {
                keys.add(taskMetaData);
            }

            for (TaskMetaData key: keys) {
                Future<Integer> future = map.get(key);
                System.out.println(key);
                if (future.isDone()) {
                    try {
                        System.out.println(String.format("Done Thread: %s", key.getTaskName()));
                        // 获取结果，如果抛出异常，说明任务运行有问题，需要进行重试
                        future.get();
                        // 任务正常运行完成，移除这个任务
                        map.remove(key);
                    } catch (ExecutionException e) {
                        // 对异常任务进行重试
                        e.printStackTrace();
                        System.out.println(String.format("ReStart submit task: %s", key.getTaskName()));
                        if (key.getRetries() > MAX_RETRIES) {
                            // 超过最大重试次数达到上限
                            throw new Exception(String.format("%s number of retries reaches the limit", key.getTaskName()));
                        } else {
                            System.out.println(String.format("线程%s进行第%d次重试", key.getTaskId(), key.getRetries() + 1));
                            key.setRetries(key.getRetries() + 1);
                            Future<Integer> retryFuture = submit(2);
                            map.put(key, retryFuture);
                        }
                    }
                }

                if (map.isEmpty()) {
                    flag = false;
                }
            }
        }

        executorService.shutdown();
    }

    public static Future<Integer> submit(int sleepTime) throws InterruptedException {
        TimeUnit.SECONDS.sleep(sleepTime);
        Future<Integer> future = executorService.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                for (; ; ) {
                    synchronized (lock) {
                        count++;
                        System.out.println(String.format("Current thread: %s count: %d", Thread.currentThread(), count));
                        if (count == 50 || count == 100) {
                            System.out.println(String.format("[Error] thread: %s", Thread.currentThread()));
                            throw new Exception(String.format("count: %d", count));
                        }

                        if (count > 200) {
                            break;
                        }
                    }
                }
                return 0;
            }
        });

        return future;
    }
}

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
class TaskMetaData{
    private Integer taskId;
    private String taskName;
    private Integer retries;
}
