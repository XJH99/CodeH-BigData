package com.codeh.search;

import java.util.Arrays;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className InsertValueSearch
 * @date 2021/7/4 21:03
 * @description 插值查找算法
 * <p>
 * 注意：
 * 1.数组必须是连续的有序数组
 * 2.算法原理类似于二分查找
 * 3.int mid = left + (right - left) * (findValue - arr[left]) / (arr[right] - arr[left])
 */
public class InsertValueSearch {

    public static void main(String[] args) {
        int[] arr = new int[100];

        for (int i = 0; i < 100; i++) {
            arr[i] = i + 1;
        }

        int res = search(arr, 0, arr.length - 1, 30);
        System.out.println("index：" + res);
        //System.out.println(Arrays.toString(arr));
    }

    public static int search(int[] arr, int left, int right, int findValue) {
        // 注意：findValue > arr[arr.length-1] ， findValue < arr[0]这两个条件必须有
        if (left > right || findValue > arr[arr.length - 1] || findValue < arr[0]) {
            return -1;
        }

        int mid = left + (right - left) * (findValue - arr[left]) / (arr[right] - arr[left]);
        int midValue = arr[mid];
        if (findValue > midValue) {
            return search(arr, mid + 1, right, findValue);
        } else if (findValue < midValue) {
            return search(arr, left, mid - 1, findValue);
        } else {
            return mid;
        }
    }
}
