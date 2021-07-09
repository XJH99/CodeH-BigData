package com.codeh.sort;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className QuickSort
 * @date 2021/6/23 17:28
 * @description 快速排序：从数列中挑出一个元素，称为 "基准"（pivot）;
 * <p>
 * 重新排序数列，所有元素比基准值小的摆放在基准前面，所有元素比基准值大的摆在基准的后面（相同的数可以到任一边）。在这个分区退出之后，该基准就处于数列的中间位置。这个称为分区（partition）操作；
 * <p>
 * 递归地（recursive）把小于基准值元素的子数列和大于基准值元素的子数列排序；
 *
 *  平均时间复杂度：O(n log n) 最好的情况：O(n log n) 最坏的情况：O(n^2)
 */
public class QuickSort {
    public static void main(String[] args) {
//        int[] arr = {10, -20, 50, 9, 30, -10};
//        quickSort(arr, 0, arr.length - 1);
//        System.out.println(Arrays.toString(arr));

        int[] arr = new int[800000];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (int) (Math.random() * 80000000);
        }
        Date date1 = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date_str1 = simpleDateFormat.format(date1);

        System.out.println("排序前的时间为：" + date_str1);

        quickSort(arr, 0, arr.length - 1);

        Date date2 = new Date();
        String date_str2 = simpleDateFormat.format(date2);
        System.out.println("排序后的时间为：" + date_str2);

    }

    public static void quickSort(int[] arr, int left, int right) {
        int l = left;   // 左索引
        int r = right;  // 右索引
        int pivot = arr[(left + right) / 2];    // 中轴值
        int temp = 0;

        while (l < r) {
            // 从中轴值的左边找到一个大于中轴值的数
            while (arr[l] < pivot) {
                l += 1;
            }

            // 从中轴值的右边找到一个小于中轴值的数
            while (arr[r] > pivot) {
                r -= 1;
            }

            // 说明左边值都是小于pivot，右边值都是大于pivot
            if (l >= r) {
                break;
            }

            // 交换数据
            temp = arr[l];
            arr[l] = arr[r];
            arr[r] = temp;

            // 如果交换完后，发现这个arr[l] == pivot 右索引前移
            if (arr[l] == pivot) {
                r -= 1;
            }

            if (arr[r] == pivot) {
                l += 1;
            }
        }
        if (l == r) {
            l += 1;
            r -= 1;
        }

        // 进行左递归
        if (left < r) {
            quickSort(arr, left, r);
        }

        // 右递归
        if (right > l) {
            quickSort(arr, l, right);
        }

    }
}
