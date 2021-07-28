package com.codeh.collection.map;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className HashMapSource
 * @date 2021/7/27 16:21
 * @description HashMap源码分析
 */
@SuppressWarnings("all")
public class HashMapSource {
    public static void main(String[] args) {
        Map map = new HashMap();

        map.put("hello", "java");
        map.put("kiss", "python");
        map.put("hello", "dw");

        System.out.println("map:" + map);

        /**
         * 1.执行无参构造
         *
         * public HashMap() {
         * 		// 初始化加载因子 loadFactor = 0.75
         *     this.loadFactor = DEFAULT_LOAD_FACTOR; // all other fields defaulted
         * }
         */

        /**
         * 2.执行put方法
         *
         * public V put(K key, V value) {
         * 		// 对key进行hash操作，计算key的hash值，hash算法：return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
         *     return putVal(hash(key), key, value, false, true);
         * }
         */

        /**
         * 3.执行putVal方法
         *
         * final V putVal(int hash, K key, V value, boolean onlyIfAbsent,
         *                    boolean evict) {
         * 				// 初始化一些变量
         *         Node<K, V>[] tab;
         *         Node<K, V> p;
         *         int n, i;
         * 				// 如果hash表数组为空或length = 0，进行扩容，扩容到16
         *         if ((tab = table) == null || (n = tab.length) == 0)
         *             n = (tab = resize()).length;
         * 				// 取出hash值对应的table的索引位置的Node，如果为null，就直接创建一个Node将数据放入即可
         *         if ((p = tab[i = (n - 1) & hash]) == null)
         *             tab[i] = newNode(hash, key, value, null);
         *         else {
         * 						// 定义一些辅助变量
         *             Node<K, V> e;
         *             K k;
         * 						// 如果table的索引位置的key的hash与新添加的key的hash相同，并同时满足（table现有节点的key和准备添加的key是同一个对象，不能添加新的k-v）
         *             if (p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k))))
         *                 e = p;
         * 						// 当前tabel的已有节点变成红黑树，按照红黑树的方式进行处理
         *             else if (p instanceof TreeNode)
         *                 e = ((TreeNode<K, V>) p).putTreeVal(this, tab, hash, key, value);
         *             else {
         * 								// 如果找到的节点后面是链表，循环进行依次比较
         *                 for (int binCount = 0; ; ++binCount) {
         *                     if ((e = p.next) == null) {
         *                         p.next = newNode(hash, key, value, null);
         * 												// 加入节点后，判断当前链表的个数，是否已到达8个，到8个后调用treeifyBin方法进行树化
         *                         if (binCount >= TREEIFY_THRESHOLD - 1) // -1 for 1st
         *                             treeifyBin(tab, hash);
         *                         break;
         *                     }
         *                     if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k))))
         *                         break;
         *                     p = e;
         *                 }
         *             }
         *             if (e != null) { // existing mapping for key
         *                 V oldValue = e.value;
         *                 if (!onlyIfAbsent || oldValue == null)
         *                     e.value = value; // 替换对应key的value
         *                 afterNodeAccess(e);
         *                 return oldValue;
         *             }
         *         }
         *         ++modCount;
         *         if (++size > threshold) // 达到临界值就进行扩容
         *             resize();
         *         afterNodeInsertion(evict);
         *         return null;
         *     }
         */
    }



}
