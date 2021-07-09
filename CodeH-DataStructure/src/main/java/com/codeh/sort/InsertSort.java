package com.codeh.sort;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className InsertSort
 * @date 2021/6/23 11:25
 * @description 插入排序：将第一待排序列第一个元素看作一个有序序列，把第二个元素到最后一个元素当成未排序序列；
 * 从头到尾依次扫描未排序序列，将扫描到的每个元素插入有序序列的适当位置
 *
 * 平均时间复杂度：O(n^2) 最好的情况：O(n) 最坏的情况：O(n^2)
 */
public class InsertSort {
    public static void main(String[] args) {
//        int[] arr = {10, -1, 4, 9, 2, 8};
//        insertSort(arr);
//        System.out.println(Arrays.toString(arr));

        int[] arr = new int[80000];
        for (int i=0; i<arr.length;i++) {
            arr[i] = (int)(Math.random() * 80000000);
        }
        Date date1 = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date_str1 = simpleDateFormat.format(date1);

        System.out.println("排序前的时间为：" + date_str1);

        insertSort(arr);

        Date date2 = new Date();
        String date_str2 = simpleDateFormat.format(date2);
        System.out.println("排序后的时间为：" + date_str2);

    }

    /**
     * 时间复杂度：O(n^2)
     *
     * @param arr 待排序数组
     */
    public static void insertSort(int[] arr) {
        int insertVal = 0;
        int insertIndex = 0;
        for (int i = 1; i < arr.length; i++) {
            insertVal = arr[i];
            insertIndex = i - 1; // 即arr[i]前面这个数的下标

            // insertIndex >= 0 保证在给insertVal找插入位置不越界
            // insertVal < arr[insertIndex] 待插入的数，说明还没有找到插入位置
            while (insertIndex >= 0 && insertVal < arr[insertIndex]) {
                arr[insertIndex + 1] = arr[insertIndex];
                insertIndex--;
            }

            // 退出循环说明插入的位置找到了
            if (insertIndex + 1 != i) {
                arr[insertIndex + 1] = insertVal;
            }


        }

    }
}
