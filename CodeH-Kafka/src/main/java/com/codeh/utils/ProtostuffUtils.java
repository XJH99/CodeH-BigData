package com.codeh.utils;

import com.codeh.bean.Company;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ProtostuffUtils
 * @date 2021/4/2 10:31
 * @description Protostuff序列化库的使用
 */
public class ProtostuffUtils {

    /**
     * 避免每次序列化都要重新申请Buffer空间,可能会线程不安全
     */
    public static LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);

    /**
     * 缓存Schema
     */
    public static Map<Class<?>, Schema<?>> schemaCache = new ConcurrentHashMap();

    /**
     * 序列化方法
     *
     * @param obj
     * @param <T>
     * @return
     */
    public static <T> byte[] serialize(T obj) {
        Class<T> aClass = (Class<T>) obj.getClass();
        Schema<T> schema = RuntimeSchema.getSchema(aClass);
        byte[] data = new byte[0];

        try {
            data = ProtobufIOUtil.toByteArray(obj, schema, buffer);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            buffer.clear();
        }

        return data;
    }


    /**
     * 反序列化方法
     * @param data
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T deserialize(byte[] data, Class<T> clazz) {
        if (data == null) {
            return null;
        }
        Schema<T> schema = RuntimeSchema.getSchema(clazz);
        T obj = schema.newMessage();
        ProtobufIOUtil.mergeFrom(data, obj, schema);

        return obj;
    }


    public static void main(String[] args) {
        // 构建对象
        Company company = Company.builder().name("张三").address("上海市").build();

        // 序列化对象
        byte[] bytes = ProtostuffUtils.serialize(company);
        System.out.println("序列化后的数据：" + Arrays.toString(bytes));

        // 反序列化后的结果
        Company result = ProtostuffUtils.deserialize(bytes, Company.class);
        System.out.println("反序列化后的数据：" + result.toString());
    }
}
