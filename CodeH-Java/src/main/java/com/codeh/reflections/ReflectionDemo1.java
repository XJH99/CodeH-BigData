package com.codeh.reflections;

import com.codeh.bean.Cat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 *
 * 反射相对于平常对象的调用方式，更加的消耗性能
 */
@SuppressWarnings("all")
public class ReflectionDemo1 {
    public static void main(String[] args) throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        m1();
        m2();
    }

    public static void m1() {
        long startTime = System.currentTimeMillis();
        Cat cat = new Cat();
        for (int i = 0; i < 900000000; i++) {
            cat.hi();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("普通方式调用方法所消耗时间：" + (endTime - startTime));
    }

    public static void m2() throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        long startTime = System.currentTimeMillis();
        Class cls = Class.forName("com.codeh.bean.Cat");
        Object instance = cls.newInstance();
        Method methodName = cls.getMethod("hi");
        methodName.setAccessible(true); // 关闭反射访问检查
        for (int i = 0; i < 900000000; i++) {
            methodName.invoke(instance);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("反射的方式调用方法所消耗的时间：" + (endTime - startTime));
    }
}
