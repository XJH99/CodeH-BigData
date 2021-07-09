package com.codeh.design_patterns.single;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SingleTon_Lazy
 * @date 2021/4/27 14:16
 * @description 单例模式之懒汉式：体现了缓存的思想，起到了延时加载的作用，但是线程不安全
 */
public class SingleTon_Lazy {

    private static SingleTon_Lazy instance = null;

    private SingleTon_Lazy(){}

    // 获取实例对象
    public static SingleTon_Lazy getInstance() {
        if (instance == null) {
            instance = new SingleTon_Lazy();
        }
        return instance;
    }
}
