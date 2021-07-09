package com.codeh.sort;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SelectSort
 * @date 2021/6/23 10:19
 * @description 选择排序：是从预排序的数据中，按指定的规则选出某一元素，再依规定交换位置后达到排序的目的
 *
 * 平均时间复杂度 O(n^2)，最好的情况：O(n^2)，最坏的情况：O(n^2)
 */
public class SelectSort {
    public static void main(String[] args) {
//        int[] arr = {10, 3, 6, 23, 9};
//        selectSort(arr);
//
//        System.out.println(Arrays.toString(arr));

        int[] arr = new int[80000];
        for (int i=0; i<arr.length;i++) {
            arr[i] = (int)(Math.random() * 80000000);
        }
        Date date1 = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date_str1 = simpleDateFormat.format(date1);

        System.out.println("排序前的时间为：" + date_str1);

        selectSort(arr);

        Date date2 = new Date();
        String date_str2 = simpleDateFormat.format(date2);
        System.out.println("排序后的时间为：" + date_str2);

    }

    /**
     * 封装选择排序方法，时间复杂度O(n^2)
     *
     * @param arr 传入的待排序数组
     */
    public static void selectSort(int[] arr) {

        for (int i = 0; i < arr.length - 1; i++) {
            int minIndex = i;
            int min = arr[i];
            for (int j = i + 1; j < arr.length; j++) {
                if (min > arr[j]) { // 判断默认的最小值是否大于后面依次比较的值
                    min = arr[j];
                    minIndex = j;
                }
            }

            // 将最小值，放在arr[0],即交换
            if (minIndex != i) {
                arr[minIndex] = arr[i];
                arr[i] = min;
            }
        }

    }
}
