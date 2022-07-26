package com.codeh.reflections;

import java.lang.reflect.Field;

/**
 * 反射操作属性
 */
public class ReflectionDemo7 {
    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        Class<?> cls = Class.forName("com.codeh.reflections.Student");

        // 1,获取实例
        Object instance = cls.newInstance();

        // 2,设置属性值，并获取属性值，获取public修饰的
        Field name = cls.getField("name");
        name.set(instance, "tom");
        System.out.println(instance);
        System.out.println(name.get(instance));

        // 3,设置属性值，并获取属性值，private修饰的，需要使用setAccessible爆破
        Field city = cls.getDeclaredField("city");
        city.setAccessible(true);
        city.set(instance, "ShangHai");
        city.set(null, "JiangXi");  // 因为属性是static的，所以传入的实例可以为null
        System.out.println(instance);
    }
}

class Student{
    public String name;
    private static String city;

    public Student() {

    }

    public String toString() {
        return "Student [name=" + name + ", city=" + city + "]";
    }
}
