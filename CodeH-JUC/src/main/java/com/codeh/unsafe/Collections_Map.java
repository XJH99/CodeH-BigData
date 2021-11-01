package com.codeh.unsafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Collections_Map
 * @date 2021/11/1 16:08
 * @description TODO
 */
public class Collections_Map {
    public static void main(String[] args) {
        /**
         * 解决方案：
         *  1.Collections.synchronizedMap
         *  2.new ConcurrentHashMap<String, String>()
         */
        //HashMap<String, String> map = new HashMap<>();
        //HashMap<String, String> map = (HashMap<String, String>) Collections.synchronizedMap(new HashMap<>());
        Map<String, String> map = new ConcurrentHashMap<String, String>();

        for (int i = 1; i <= 30; i++) {
            new Thread(() -> {
                map.put(Thread.currentThread().getName(), UUID.randomUUID().toString().substring(0, 5));
                System.out.println(map);
            }, String.valueOf(i)).start();
        }
    }
}
