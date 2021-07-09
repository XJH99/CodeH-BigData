package com.codeh.sort;

import org.junit.Test;

import java.util.Arrays;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SortTest
 * @date 2021/6/25 11:20
 * @description 排序算法练习
 */
public class SortTest {

    /**
     * 冒泡排序：时间复杂度O(n^2)
     */
    @Test
    public void bubbleSort() {
        int[] arr = {8, 9, 1, 7, 2, 3, 5, 4, 6, 0};
        int temp;
        for (int i = 0; i < arr.length - 1; i++) {
            for (int j = 0; j < arr.length - i - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
        System.out.println("冒泡排序后的数组：" + Arrays.toString(arr));
    }

    /**
     * 选择排序：时间复杂度O(n^2)
     */
    @Test
    public void selectSort() {
        int[] arr = {8, 9, 1, 7, 2, 3, 5, 4, 6, 0};

        for (int i = 0; i < arr.length - 1; i++) {
            int minIndex = i;
            int min = arr[i];


            for (int j = i + 1; j < arr.length; j++) {
                if (min > arr[j]) {
                    minIndex = j;
                    min = arr[j];
                }
            }

            if (minIndex != i) {
                arr[minIndex] = arr[i];
                arr[i] = min;
            }
        }

        System.out.println("选择排序后的数组：" + Arrays.toString(arr));
    }

    /**
     * 插入排序：时间复杂度O(n^2)
     */
    @Test
    public void insertSort() {
        int[] arr = {8, 9, 1, 7, 2, 3, 5, 4, 6, 0};
        int insertIndex = 0;
        int insertVal = 0;
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

        System.out.println("插入排序后的数组：" + Arrays.toString(arr));
    }

    /**
     * 快速排序：时间复杂度O(nlogn)
     */
    @Test
    public void quickSort() {
        int[] arr = {8, 9, 1, 7, 2, 3, 5, 4, 6, 0};
        int left = 0;
        int right = arr.length -1;

    }

}
