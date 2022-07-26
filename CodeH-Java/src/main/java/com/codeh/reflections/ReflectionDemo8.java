package com.codeh.reflections;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 反射操作方法
 */
public class ReflectionDemo8 {
    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Class<?> aClass = Class.forName("com.codeh.reflections.Boss");

        Object instance = aClass.newInstance();

        // 访问public方法，调用invoke方法执行
        Method hi = aClass.getMethod("hi", String.class);
        hi.invoke(instance, "come");

        // 访问private修饰的方法，需要setAccessible爆破处理
        Method say = aClass.getDeclaredMethod("say", int.class, String.class, char.class);
        say.setAccessible(true);
        System.out.println(say.invoke(instance, 10, "show time", 'p'));
        // 因为对应的方法是静态的，所以可以传入的实例对象为null值
        System.out.println(say.invoke(null, 20, "come on", 'p'));

        // invoke方法调用返回的是一个object对象，但是运行实例还是String
        Object o = say.invoke(instance, 99, "baby", 'q');
        System.out.println(o.getClass());


    }
}

class Boss{
    public int age;
    private static String name;

    public Boss() {

    }

    private static String say (int n, String s, char c) {
        return n + " " + s + " " + c;
    }

    public void hi(String s) {
        System.out.println("hi " + s);
    }
}
