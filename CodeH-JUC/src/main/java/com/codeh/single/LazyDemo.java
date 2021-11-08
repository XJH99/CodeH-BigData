package com.codeh.single;

import java.lang.reflect.Constructor;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className LazyDemo
 * @date 2021/11/8 15:10
 * @description 单例模式之懒汉式,反射破坏不了枚举的值
 */
public class LazyDemo {
    private LazyDemo() {
        synchronized (LazyDemo.class) {
            if (instance != null) {
                throw new RuntimeException("不要尝试使用反射破坏单例");
            }
        }
        System.out.println(Thread.currentThread().getName() + "ok");
    }

    private volatile static LazyDemo instance;

    // 双重检测锁DCL懒汉式单例
    public static LazyDemo getInstance() {
        if (instance == null) {
            synchronized (LazyDemo.class) {
                if (instance == null) {
                    instance = new LazyDemo();  // 不是一个原子性操作
                    /**
                     * 有可能指令重排
                     * 1.分配内存空间
                     * 2.执行构造方法，初始化对象
                     * 3.把这个对象指向这个空间
                     */
                }
            }
        }
        return instance;
    }

    public static void main(String[] args) throws Exception {
        // 通过反射破环单例
        LazyDemo instance1 = LazyDemo.getInstance();
        Constructor<LazyDemo> constructor = LazyDemo.class.getDeclaredConstructor(null);
        constructor.setAccessible(true);
        LazyDemo instance2 = constructor.newInstance();
        LazyDemo instance3 = constructor.newInstance();
        System.out.println(instance3);
        System.out.println(instance2);
    }


}
