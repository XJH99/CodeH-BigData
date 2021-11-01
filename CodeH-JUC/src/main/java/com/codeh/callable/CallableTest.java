package com.codeh.callable;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className CallableTest
 * @date 2021/11/1 17:03
 * @description callable的使用
 * 特点：
 *      1.有返回值
 *      2.可能抛出异常
 */
public class CallableTest {
    public static void main(String[] args) {
        // 启动callable借用FutureTask
        // FutureTask是Runnable的具体实现类
        new Thread().start();
        MyThread thread = new MyThread();
        FutureTask futureTask = new FutureTask(thread);

        new Thread(futureTask, "A").start();
        new Thread(futureTask, "B").start(); //会有一个缓存的效果
        try {
            Integer res = (Integer) futureTask.get(); // get方法会产生阻塞效果，把他放在最后
            System.out.println(res);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

class MyThread implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        System.out.println("call()");
        return 1024;
    }
}