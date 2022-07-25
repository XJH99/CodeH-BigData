package com.codeh.reflections;

import com.codeh.bean.Car;

/**
 * 获取class的几种方式：其实主要还是三种
 */
public class ReflectionDemo4 {
    public static void main(String[] args) throws ClassNotFoundException {
        // 1.通过class.forName的方式进行获取
        Class<?> cls1 = Class.forName("com.codeh.bean.Car");
        System.out.println(cls1);

        // 2.通过类名直接获取
        Class<Car> cls2 = Car.class;
        System.out.println(cls2);

        //3.通过实例对象getClass方法获取
        Car car = new Car();
        System.out.println(car.getClass());

        //4.通过类加载器进行获取
        ClassLoader classLoader = car.getClass().getClassLoader();
        Class<?> cls3 = classLoader.loadClass("com.codeh.bean.Car");
        System.out.println(cls3);

        // 5.包装类通过Type获取
        Class<Integer> type = Integer.TYPE;
        Class<Character> type1 = Character.TYPE;
        System.out.println(type);
        System.out.println(type1);

        // 6.基本数据类型的方式获取
        Class<Integer> intClass = int.class;
        Class<Double> doubleClass = double.class;
        System.out.println(intClass);
        System.out.println(doubleClass);

        System.out.println(intClass == type); // True
    }
}
