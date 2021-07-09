package com.codeh.design_patterns.single;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SingleTon_Lazy_Sync
 * @date 2021/4/27 14:40
 * @description 线程安全的懒汉式：双重检查锁
 */
public class SingleTon_Lazy_Sync {

    private static SingleTon_Lazy_Sync instance = null;

    private SingleTon_Lazy_Sync() {}

    public static SingleTon_Lazy_Sync getInstance() {
        if (instance == null) {
            // 当拿到这个类的锁时才能创建实例
            synchronized (SingleTon_Lazy_Sync.class) {
                if (instance == null) {
                    return new SingleTon_Lazy_Sync();
                }
            }
        }
        return instance;
    }
}
