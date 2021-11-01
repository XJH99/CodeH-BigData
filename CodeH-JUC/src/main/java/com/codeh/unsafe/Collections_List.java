package com.codeh.unsafe;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Collections_List
 * @date 2021/11/1 15:21
 * @description 不安全集合类之List
 * java.util.ConcurrentModificationException 并发修改异常
 */
public class Collections_List {
    public static void main(String[] args) {
        /**
         * 解决方案：
         * 1.使用List<String> list = new Vector<>();线程安全集合类, add方法使用了synchronized
         * 2.使用集合的工具类，将ArrayList转为线程安全的 List<String> list = Collections.synchronizedList(new ArrayList<>());
         * 3.JUC中的List<String> list = new CopyOnWriteArrayList<>();
         *      CopyOnWriteArrayList写入时复制，COW是计算机程序设计领域的一种优化策略
         *      多个线程调用的时候，list，读取的时候固定的写入；在写入的时候避免覆盖，造成数据问题
         */
        //List<String> list = new Vector<>();
        //List<String> list = Collections.synchronizedList(new ArrayList<>());
        List<String> list = new CopyOnWriteArrayList<>();


        for (int i = 1; i <= 10; i++) {
            new Thread(() -> {
                list.add(UUID.randomUUID().toString().substring(0, 5));
                System.out.println(list);
            }, String.valueOf(i)).start();
        }
    }
}
