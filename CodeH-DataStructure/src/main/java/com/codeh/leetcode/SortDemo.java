package com.codeh.leetcode;

import java.util.Arrays;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SortDemo
 * @date 2022/7/18 16:24
 * @description TODO
 */
public class SortDemo {
    public static void main(String[] args) {
        int[] arr = {0, -1, 9, -10, 3};
        System.out.println(Arrays.toString(insertSort(arr)));

    }

    /**
     * 插入排序
     *
     * @param arr 待排序数组
     * @return 排序好的数组
     */
    public static int[] insertSort(int[] arr) {
        if (arr == null || arr.length < 2)
            return arr;

        int insertVal = 0;
        int insertIndex = 0;
        for (int i = 1; i < arr.length; i++) {
            insertVal = arr[i];
            insertIndex = i - 1;

            while (insertIndex >= 0 && insertVal < arr[insertIndex]) {
                arr[insertIndex + 1] = arr[insertIndex];
                insertIndex--;
            }

            if (insertIndex + 1 != i) {
                arr[insertIndex + 1] = insertVal;
            }
        }

        return arr;
    }
}
