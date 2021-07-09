package com.codeh.design_patterns.single;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SingleTon01_Hungry
 * @date 2021/4/27 11:55
 * @description 单例模式之饿汉式，当类加载的时候就会创建类实例，空间换时间的概念，不存在线程安全的问题
 */
public class SingleTon_Hungry {

    // 创建实例对象
    private static SingleTon_Hungry instance = new SingleTon_Hungry();

    // 构造私有化
    private SingleTon_Hungry() {}

    // 返回实例对象
    public static SingleTon_Hungry getInstance() {
        return instance;
    }
}
