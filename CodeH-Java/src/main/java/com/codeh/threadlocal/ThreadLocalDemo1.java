package com.codeh.threadlocal;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ThreadLocalDemo1
 * @date 2022/7/19 16:25
 * @description ThreadLocal的使用
 *
 * 特点：
 *      1.线程并发：在多线程并发的场景下
 *      2.传递数据：我们可以通过ThreadLocal在同一线程，不同组件中传递公共变量
 *      3.线程隔离：每个线程的变量都是独立的，不会相互影响
 *
 *      set():将变量绑定到当前线程当中
 *      get():获取当前线程绑定的变量
 */
public class ThreadLocalDemo1 {

    ThreadLocal<String> threadLocal = new ThreadLocal<>();

    public void setContent(String content) {
        threadLocal.set(content);
    }

    public String getContent() {
        return threadLocal.get();
    }

    public static void main(String[] args) {
        ThreadLocalDemo1 threadLocalDemo1 = new ThreadLocalDemo1();
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    threadLocalDemo1.setContent(Thread.currentThread().getName() + "的数据");
                    System.out.println("----------------------------------------");
                    System.out.println(Thread.currentThread().getName() + "---->" + threadLocalDemo1.getContent());
                }
            });
            thread.setName("线程" + i);
            thread.start();
        }
    }
}
