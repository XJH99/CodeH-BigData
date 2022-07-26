package com.codeh.reflections;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * 反射操作构造器
 */
public class ReflectionDemo6 {
    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        // 1，获取class对象
        Class<?> userClass = Class.forName("com.codeh.reflections.User");

        // 2，访问默认无参构造
        Object instance = userClass.newInstance();
        System.out.println(instance);

        // 3，访问有参构造，只能获取public
        Constructor<?> constructor = userClass.getConstructor(String.class);
        Object instance1 = constructor.newInstance("hello pro");
        System.out.println(instance1);

        // 4，访问有参构造，并且能访问private修饰的构造，使用需要爆破setAccessible()
        Constructor<?> constructor1 = userClass.getDeclaredConstructor(String.class, int.class);
        constructor1.setAccessible(true);
        Object instance2 = constructor1.newInstance("hello mac pro", 20);
        System.out.println(instance2);


    }
}

class User{
    private String name = "hell mac";
    private int age = 10;

    public User() {
    }

    public User(String name) {
        this.name = name;
    }

    private User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
