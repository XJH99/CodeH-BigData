package com.codeh.sort;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className BubbleSort
 * @date 2021/6/22 18:10
 * @description 冒泡排序：对待排序序列从前向后，依次比较相邻元素的值，若发现逆序则交换，使值较大的元素逐渐从前移向后部
 *
 * 平均时间复杂度 O(n^2)，最好的情况：O(n)，最坏的情况：O(n^2)
 */
public class BubbleSort {
    public static void main(String[] args) {
//        int[] arr = {3, 9, -1, 10, 8};
//
//        System.out.println("排序前的数组如下:" + Arrays.toString(arr));
//        bubbleSort(arr);
//        System.out.println("排序后的数组如下：" + Arrays.toString(arr));

        int[] arr = new int[80000];
        for (int i=0; i<arr.length;i++) {
            arr[i] = (int)(Math.random() * 80000000);
        }
        Date date1 = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date_str1 = simpleDateFormat.format(date1);

        System.out.println("排序前的时间为：" + date_str1);

        bubbleSort(arr);

        Date date2 = new Date();
        String date_str2 = simpleDateFormat.format(date2);
        System.out.println("排序后的时间为：" + date_str2);


    }

    /**
     * 冒泡排序封装方法，时间复杂度O(n^2)
     * @param arr 传入待排序的数组
     */
    public static void bubbleSort(int[] arr) {
        int temp;
        boolean flag = false;   // 标识变量，表示是否进行过交换

        for (int i = 0; i < arr.length - 1; i++) {
            for (int j = 0; j < arr.length - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    flag = true;
                    temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
            if (!flag) { // 一次排序中，一次交换都没有发生，直接跳出
                break;
            } else {
                flag = false;
            }
        }
    }
}
