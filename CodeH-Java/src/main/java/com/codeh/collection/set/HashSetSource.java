package com.codeh.collection.set;

import java.util.HashSet;
import java.util.Set;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className HashSetSource
 * @date 2021/7/26 15:13
 * @description HashSet源码解读
 */
@SuppressWarnings("all")
public class HashSetSource {
    public static void main(String[] args) {
        Set hashSet = new HashSet();

        hashSet.add("java");
        hashSet.add("php");
        hashSet.add("java");

        System.out.println("set:" + hashSet);

        /**
         * 1.使用无参构造器创建了一个hashMap
         * public HashSet() {
         *         map = new HashMap<>();
         * }
         */

        /**
         * 2.调用add方法
         * public boolean add(E e) {
         *     return map.put(e, PRESENT)==null;
         * }
         */

        /**
         * 3.执行put方法
         * public V put(K key, V value) {
         *     return putVal(hash(key), key, value, false, true);
         * }
         */

        /**
         * 4.执行putVal方法
         * final V putVal(int hash, K key, V value, boolean onlyIfAbsent, boolean evict) {
         *         Node<K, V>[] tab;
         *         Node<K, V> p;
         *         int n, i;    // 定义的一些辅助变量
         *         if ((tab = table) == null || (n = tab.length) == 0) // table是hashMap的一个数组，类型为Node<K,V>[] table
         *             n = (tab = resize()).length; // 第一次执行时表示为空数组，进行扩容，第一次扩容到16个空间 static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; 加载因子为12
         *         if ((p = tab[i = (n - 1) & hash]) == null)
         *             tab[i] = newNode(hash, key, value, null); // 根据key，得到hash值去计算该key应该存放在table表的那个索引位置，如果p为空，表示还没有存放元素，就创建一个node节点并存放在当前索引的数组位置
         *         else {
         *             Node<K, V> e;
         *             K k;
         *             // 如果当前索引位置对应的链表在第一个元素和准备添加的key的hash值一样
         *             if (p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k))))
         *                 e = p;
         *             // 如果p是一颗红黑树，调用putTreeVal进行添加处理
         *             else if (p instanceof TreeNode)
         *                 e = ((TreeNode<K, V>) p).putTreeVal(this, tab, hash, key, value);
         *             else {
         *                 // 如果table对应索引位置，已经是一个链表，就使用for循环进行比较
         *                 for (int binCount = 0; ; ++binCount) {
         *                     // 依次和该链表的每一个元素比较后，都不相同，则加入到该链表的最后
         *                     if ((e = p.next) == null) {
         *                         p.next = newNode(hash, key, value, null);
         *                         if (binCount >= TREEIFY_THRESHOLD - 1) // -1 for 1st
         *                             treeifyBin(tab, hash); //链表达到8个节点，数组容量达到64，链表树化
         *                         break;
         *                     }
         *                     if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k))))
         *                         break;
         *                     p = e;
         *                 }
         *             }
         *             // e不为空表示有重复元素
         *             if (e != null) { // existing mapping for key
         *                 V oldValue = e.value;
         *                 if (!onlyIfAbsent || oldValue == null)
         *                     e.value = value;
         *                 afterNodeAccess(e);
         *                 return oldValue;
         *             }
         *         }
         *         ++modCount;
         *         if (++size > threshold) resize(); // 如果hash表中的元素个数大于加载因子数值，进行扩容
         *         afterNodeInsertion(evict); // 为LinkedHashSet保留的方法
         *         return null;
         *     }
         */

        /**
         * 5.一些常量
         * // 加载因子
         * static final float DEFAULT_LOAD_FACTOR = 0.75f;
         *
         * // 第一次扩容大小16
         * static final int DEFAULT_INITIAL_CAPACITY = 1 << 4;
         *
         * // 树化最小的表容量
         * static final int MIN_TREEIFY_CAPACITY = 64;
         *
         * // 临界扩容值
         * threshold = (int)(DEFAULT_LOAD_FACTOR * DEFAULT_INITIAL_CAPACITY);
         */
    }


}
