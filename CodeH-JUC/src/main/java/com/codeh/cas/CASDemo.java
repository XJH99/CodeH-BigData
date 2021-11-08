package com.codeh.cas;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className CASDemo
 * @date 2021/11/8 15:43
 * @description CAS(Compare and set / Compare and swap) 一个是Java层面，一个是底层
 * 缺点：
 *      1.底层的自旋锁循环会很耗时
 *      2.一次性只能保证一个共享变量的原子性
 *      3.ABA问题
 */
public class CASDemo {
    public static void main(String[] args) {
        AtomicInteger atomicInteger = new AtomicInteger(2020);
        // 如果期望值达到了，那么就更新，否则就不更新；CAS是CPU的并发原语
        atomicInteger.compareAndSet(2020, 2021);
        System.out.println(atomicInteger.get());
    }
}
