package com.codeh.reflections;

import java.lang.reflect.Field;

/**
 * 反射的常用方法
 */
public class ReflectionDemo3 {
    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        Class<?> cls = Class.forName("com.codeh.bean.Car");
        System.out.println(cls);
        System.out.println(cls.getClass());

        // 1.得到包名
        String packageName = cls.getPackage().getName();
        System.out.println(packageName);

        // 2.得到全类名
        System.out.println(cls.getName());

        // 3.得到cls创建的对象实例
        Object instance = cls.newInstance();
        System.out.println(instance);

        // 4.反射获取属性
        Field brand = cls.getField("brand");
        System.out.println(brand.get(instance));

        // 5.通过反射给属性赋值
        brand.set(instance, "奔驰");
        System.out.println(brand.get(instance));

        // 6.获取所有的属性
        Field[] fields = cls.getFields();
        for (Field field: fields) {
            System.out.println(field.getName());
        }

    }
}
