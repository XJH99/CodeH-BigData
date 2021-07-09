package com.codeh.sort;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ShellSort
 * @date 2021/6/23 15:32
 * @description 希尔排序：是把记录按下标的一定增量分组，对每组使用直接插入排序算法排序；随着增量逐渐减少，每组的关键词越来越多，当增量减至1时，整个文件恰被分成一组，算法终止
 *
 * 平均时间复杂度 O(n log n)，最好的情况：O(n log^2 n)，最坏的情况：O(n log^2 n)
 */
public class ShellSort {
    public static void main(String[] args) {
//        int[] arr = {8, 9, 1, 7, 2, 3, 5, 4, 6, 0};
//        //shellSort(arr);
//        shellSort2(arr);

        int[] arr = new int[80000];
        for (int i=0; i<arr.length;i++) {
            arr[i] = (int)(Math.random() * 80000000);
        }
        Date date1 = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date_str1 = simpleDateFormat.format(date1);

        System.out.println("排序前的时间为：" + date_str1);

        shellSort2(arr);

        Date date2 = new Date();
        String date_str2 = simpleDateFormat.format(date2);
        System.out.println("排序后的时间为：" + date_str2);
    }

    /**
     * 希尔排序时，对有序序列在插入时采用交换法
     *
     * @param arr 待排序数组
     */
//    public static void shellSort(int[] arr) {
//
//        int temp = 0;
//        int count = 0;
//        for (int gap = arr.length / 2; gap > 0; gap /= 2) {
//            for (int i = gap; i < arr.length; i++) {
//                // 遍历各组中所有的元素（共gap组，每组有个元素），步长gap
//                for (int j = i - gap; j >= 0; j -= gap) {
//                    // 如果当前元素大于加上步长后的那个元素，说明进行交换
//                    if (arr[j] > arr[j+gap]) {
//                        temp = arr[j];
//                        arr[j] = arr[j+gap];
//                        arr[j+gap] = temp;
//                    }
//                }
//            }
//            System.out.println("希尔排序第"+ (++count) + "轮=" + Arrays.toString(arr));
//        }
////        // 第一轮排序
////        for (int i = 5; i < arr.length; i++) {
////            for (int j = i - 5; j >= 0; j -= 5) {
////                if (arr[j] > arr[j+5]) {
////                    temp = arr[j];
////                    arr[j] = arr[j+5];
////                    arr[j+5] = temp;
////                }
////            }
////        }
////
////        System.out.println("第一轮排序如下:" + Arrays.toString(arr));
////
////        // 第一轮排序
////        for (int i = 2; i < arr.length; i++) {
////            for (int j = i - 2; j >= 0; j -= 2) {
////                if (arr[j] > arr[j+2]) {
////                    temp = arr[j];
////                    arr[j] = arr[j+2];
////                    arr[j+2] = temp;
////                }
////            }
////        }
////
////        System.out.println("第二轮排序如下:" + Arrays.toString(arr));
//    }


    /**
     * 采用希尔排序中的位移法来进行排序
     *
     * @param arr 待排序数组
     */
    public static void shellSort2(int[] arr) {
        //int count = 0;
        // gap表示步长，逐步的缩小增量步长
        for (int gap = arr.length / 2; gap > 0; gap /= 2) {
            // 从第gap个元素开始，逐个对其所在组进行直接插入排序
            for (int i = gap; i < arr.length; i++) {
                int j = i;
                int temp = arr[j];
                if (arr[j] < arr[j - gap]) {
                    while (j-gap >= 0 && temp < arr[j-gap]) {
                        // 移动
                        arr[j] = arr[j-gap];
                        j-=gap;
                    }
                    // 退出while后，就给temp找到插入的位置
                    arr[j] = temp;
                }
            }
            //System.out.println("希尔排序第"+ (++count) + "轮=" + Arrays.toString(arr));
        }
    }
}
