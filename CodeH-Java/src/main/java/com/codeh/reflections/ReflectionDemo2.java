package com.codeh.reflections;

import com.codeh.bean.Cat;

public class ReflectionDemo2 {
    public static void main(String[] args) throws ClassNotFoundException {
        // 1.Class也是类，因此也继承了Object类

        // 2.Class类对象不是new出来的，而是系统创建的
        /**
         * public Class<?> loadClass(String name) throws ClassNotFoundException {
         *         return loadClass(name, false);
         *     }
         */
        // Cat cat = new Cat();

        Class<?> aClass = Class.forName("com.codeh.bean.Cat");

        // 3.对于某个class类对象，在内存中只有一份，因为类只加载一次
    }
}
