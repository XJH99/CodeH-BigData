package com.codeh.sort;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className RadixSort
 * @date 2021/6/27 21:46
 * @description 基数排序：将所有待比较数值统一为同样的数位长度，数位较短的数前面补零；然后，从低位开始，依次进行排序，这样从最低位排序一直到最高位排序完成以后，数列就变成了一个有序序列
 *
 * 注意：
 *      基数排序是经典的空间换时间的方式，占用内存很大，当对海量数据排序时，容易造成 OutOfMemoryError
 *
 *      平均时间复杂度：O(n * k) 最好的情况：O(n * k) 最坏的情况：O(n * k)
 */
public class RadixSort {
    public static void main(String[] args) {
//        int[] arr = {53, 3, 542, 748, 14, 214};
//        radixSort(arr);
//
//        System.out.println("基数排序后的数组：" + Arrays.toString(arr));

        int[] arr = new int[8000000];
        for (int i=0; i<arr.length;i++) {
            arr[i] = (int)(Math.random() * 80000000);
        }
        Date date1 = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date_str1 = simpleDateFormat.format(date1);

        System.out.println("排序前的时间为：" + date_str1);

        radixSort(arr);

        Date date2 = new Date();
        String date_str2 = simpleDateFormat.format(date2);
        System.out.println("排序后的时间为：" + date_str2);
        //System.out.println(Arrays.toString(arr));
    }


    public static void radixSort(int[] arr) {
        // 获取最大值
        int max = arr[0];
        for (int i = 1; i < arr.length; i++) {
            if (max < arr[i]) {
                max = arr[i];
            }
        }

        // 获取最大值的长度
        int maxLength = (max + "").length();

        // 1.定义一个二维数组，每个一维数组都是一个桶
        int[][] bucket = new int[10][arr.length];

        // 2.用于记录每个桶存放元素的个数
        int[] bucketElementCounts = new int[10];

        for (int i = 0, n = 1; i < maxLength; i++, n = n * 10) {
            // 3.数组遍历操作
            for (int j = 0; j < arr.length; j++) {
                int digit = arr[j] / n % 10;
                // 将数据放入到对应的桶中
                bucket[digit][bucketElementCounts[digit]] = arr[j];
                bucketElementCounts[digit]++;
            }

            // 4.按照桶的顺序，将数取出放在原来的数组中
            int index = 0;
            for (int k = 0; k < bucket.length; k++) {
                // 遍历每一个桶，将桶中数据取出;判断桶中是否有元素
                if (bucketElementCounts[k] != 0) {
                    for (int m = 0; m < bucketElementCounts[k]; m++) {
                        arr[index] = bucket[k][m];
                        index++;
                    }
                }
                bucketElementCounts[k] = 0;
            }
        }
/*
        // 1.定义一个二维数组，每个一维数组都是一个桶
        int[][] bucket = new int[10][arr.length];

        // 2.用于记录每个桶存放元素的个数
        int[] bucketElementCounts = new int[10];

        // 3.数组遍历操作
        for (int j = 0; j < arr.length; j++) {
            int digit = arr[j] % 10;
            // 将数据放入到对应的桶中
            bucket[digit][bucketElementCounts[digit]] = arr[j];
            bucketElementCounts[digit]++;
        }

        // 4.按照桶的顺序，将数取出放在原来的数组中
        int index = 0;
        for (int k = 0; k < bucketElementCounts.length; k++) {
            // 遍历每一个桶，将桶中数据取出
            if (bucketElementCounts[k] != 0) {
                for (int m = 0; m < bucketElementCounts[k]; m++) {
                    arr[index] = bucket[k][m];
                    index++;
                }
            }
            bucketElementCounts[k] = 0;
        }

        System.out.println("第一轮排序：" + Arrays.toString(arr));

*/
    }

}
