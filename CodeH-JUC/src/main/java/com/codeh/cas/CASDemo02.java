package com.codeh.cas;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className CASDemo02
 * @date 2021/11/8 16:03
 * @description 原子引用
 */
public class CASDemo02 {
    public static void main(String[] args) {
        // 注意：如果泛型是一个包装类，注意对象的引用问题
        AtomicStampedReference<Integer> reference = new AtomicStampedReference<>(1, 1);

        new Thread(() -> {
            int stamp = reference.getStamp();   // 获取版本号
            System.out.println("a1 => " + stamp);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(reference.compareAndSet(1, 2, reference.getStamp(), reference.getStamp() + 1));

            System.out.println("a2 => " + reference.getStamp());

            System.out.println(reference.compareAndSet(2, 1, reference.getStamp(), reference.getStamp() + 1));

            System.out.println("a3 => " + reference.getStamp());
        }, "a1").start();

        new Thread(() -> {
            int stamp = reference.getStamp();   // 获取版本号
            System.out.println("b1 => " + stamp);

            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(reference.compareAndSet(1, 6, stamp, stamp + 1));
            System.out.println("b2 => " + reference.getStamp());

        }, "b1").start();
    }
}
