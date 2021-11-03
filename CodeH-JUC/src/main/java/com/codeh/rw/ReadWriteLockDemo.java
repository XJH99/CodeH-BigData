package com.codeh.rw;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ReadWriteLockDemo
 * @date 2021/11/3 14:46
 * @description 读写锁的使用
 * 独占锁（写锁）
 * 共享锁（读锁）
 * 读 - 读 ： 可以共存
 * 读 - 写 ： 不能共存
 * 写 - 写 ： 不能共存
 */
public class ReadWriteLockDemo {
    public static void main(String[] args) {
        MyCacheLock myCacheLock = new MyCacheLock();
        // 写入
        for (int i = 0; i < 5; i++) {
            final int tmp = i;
            new Thread(() -> {
                myCacheLock.put(tmp + "", tmp + "");
            }, String.valueOf(i)).start();
        }

        // 读取
        for (int i = 0; i < 5; i++) {
            final int tmp = i;
            new Thread(() -> {
                myCacheLock.get(tmp + "");
            }, String.valueOf(i)).start();
        }


    }
}

class MyCacheLock {
    private volatile Map<String, String> map = new HashMap<>();
    // 读写锁，更加细粒度的控制
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * 写入数据
     * @param key 写入的key
     * @param value 写入的value
     */
    public void put(String key, String value) {
        // 加上写锁
        readWriteLock.writeLock().lock();
        try {
            System.out.println("线程" + Thread.currentThread().getName() + "开始写入" + key);
            map.put(key, value);
            System.out.println("线程" + Thread.currentThread().getName() + "写入" + key + "成功");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放写锁
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * 读取数据
     * @param key 读取的key
     */
    public void get(String key) {
        // 加上读锁
        readWriteLock.readLock().lock();
        try {
            System.out.println("线程" + Thread.currentThread().getName() + "开始读取" + key);
            String s = map.get(key);
            System.out.println("线程" + Thread.currentThread().getName() + "读取" + key + "成功");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放读锁
            readWriteLock.readLock().unlock();
        }
    }
}
