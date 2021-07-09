package com.codeh.sort;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className MergeSort
 * @date 2021/6/27 14:13
 * @description 归并排序：分是将问题分成一些小的问题然后递归求解，治是将分的阶段得到的各答案修补在一起
 *
 *  平均时间复杂度：O(n log n) 最好的情况：O(n log n) 最坏的情况：O(n log n)
 */
public class MergeSort {
    public static void main(String[] args) {
//        int[] arr = {8, 4, 5, 7, 1, 3, 6, 2};
//        int[] temp = new int[arr.length];
//
//        mergeSort(arr, 0, arr.length - 1, temp);
//
//        System.out.println("排序后的数组：" + Arrays.toString(arr));

        int[] arr = new int[800000];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (int) (Math.random() * 80000000);
        }
        Date date1 = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date_str1 = simpleDateFormat.format(date1);

        System.out.println("排序前的时间为：" + date_str1);
        int[] temp = new int[arr.length];

        mergeSort(arr, 0, arr.length - 1, temp);

        Date date2 = new Date();
        String date_str2 = simpleDateFormat.format(date2);
        System.out.println("排序后的时间为：" + date_str2);

    }

    /**
     * 分解的方法
     *
     * @param arr   待排序数组
     * @param left  数组最左边索引
     * @param right 数组最右边索引
     * @param temp  临时数组
     */
    public static void mergeSort(int[] arr, int left, int right, int[] temp) {
        if (left < right) {
            int mid = (left + right) / 2;

            // 左边递归进行分解
            mergeSort(arr, left, mid, temp);

            // 右边递归进行分解
            mergeSort(arr, mid + 1, right, temp);

            // 合并
            merge(arr, left, mid, right, temp);
        }
    }


    /**
     * 合并的方法
     *
     * @param arr   待排序数组
     * @param left  左边有序序列的初始索引
     * @param mid   中间索引
     * @param right 右边索引
     * @param temp  做中转的数组
     */
    public static void merge(int[] arr, int left, int mid, int right, int[] temp) {
        int i = left;
        int j = mid + 1;
        int t = 0;

        // 1.先把左右两边的数据按照规则填充到temp数组中，直到左右两边的有序序列，有一边处理完毕为止
        while (i <= mid && j <= right) {
            if (arr[i] < arr[j]) {
                temp[t] = arr[i];
                t++;
                i++;
            } else {
                temp[t] = arr[j];
                t++;
                j++;
            }
        }

        // 2.把有剩余数据的一边数据依次全部填充到temp数组中
        while (i <= mid) {
            temp[t] = arr[i];
            t++;
            i++;
        }

        while (j <= right) {
            temp[t] = arr[j];
            t++;
            j++;
        }

        // 3.将temp的数组拷贝到arr
        // 注意：并不是每次都拷贝所有
        t = 0;
        int tempLeft = left;

        while (tempLeft <= right) {
            arr[tempLeft] = temp[t];
            tempLeft++;
            t++;
        }


    }


}
