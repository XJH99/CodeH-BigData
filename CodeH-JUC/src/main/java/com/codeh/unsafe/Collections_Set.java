package com.codeh.unsafe;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Collections_Set
 * @date 2021/11/1 15:51
 * @description 不安全集合Set
 * java.util.ConcurrentModificationException: 并发修改异常
 */
public class Collections_Set {
    public static void main(String[] args) {
        /**
         * 替代方案：
         *  1.Collections.synchronizedSet(new HashSet<>());
         *  2.new CopyOnWriteArraySet<>();
         */
        //Set<String> set = new HashSet<>();
        //Set<String> set = Collections.synchronizedSet(new HashSet<>());
        Set<String> set = new CopyOnWriteArraySet<>();

        for (int i = 1; i <= 30; i++) {
            new Thread(() -> {
                set.add(UUID.randomUUID().toString().substring(0, 5));
                System.out.println(set);
            }, String.valueOf(i)).start();
        }
    }
}
