package com.codeh.reflections;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * 反射的优缺点
 *      优点：可以动态的创建和使用对象（也是框架的底层核心），使用灵活，没有反射机制，框架机制就失去底层支撑
 *      缺点：使用反射基本是解释执行，对执行速度有影响
 *
 * 静态加载与动态加载的区别
 *      静态加载：依赖性很强，在编译的时候就会加载相关的类，如果没有则报错
 *      动态加载：反射是动态加载，相当于延时加载，在运行时才会加载需要的类，如果运行时不用该类，则不报错，降低了依赖性
 *
 */
@SuppressWarnings("all")
public class ReflectionDemo {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, NoSuchFieldException {

        // 1.读取配置文件
        Properties properties = new Properties();
        properties.load(new FileInputStream("CodeH-Java/src/properties.properties"));
        String path = properties.get("classpath").toString();
        String methodName = properties.get("method").toString();
        System.out.println(path + "----" + methodName);

        // 2.通过反射创建实例对象
        Class cls = Class.forName(path);
        // 2.1.通过cls得到加载类的对象实例
        Object instance = cls.newInstance();
        // 2.2.通过cls得到加载类的方法实例对象
        Method method = cls.getMethod(methodName);
        method.invoke(instance); // 方法调用实例

        // 2.3 通过cls获取成员变量
        Field fieldName = cls.getField("age");
        System.out.println(fieldName.get(instance));

        // 2.4 通过cls获取带有String参数的构造器
        Constructor constructor = cls.getConstructor(String.class);
        System.out.println(constructor);
    }
}
