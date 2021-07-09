package com.codeh.search;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className binarySearch
 * @date 2021/7/2 18:31
 * @description 二分查找算法：前提条件数组为有序数组
 */
public class binarySearch {
    public static void main(String[] args) {
        int[] arr = {1, 2, 3, 4, 5, 5, 5, 6, 7, 8, 9, 0};

//        int index = getIndex(arr, 0, arr.length - 1, 5);
//        System.out.println("查找数据对应的位置如下：" + index);

        List<Integer> list = getData(arr, 0, arr.length - 1, 5);
        System.out.println(list);

    }

    /**
     * @param arr   查询数组
     * @param left  最左边索引
     * @param right 最右边索引
     * @param value 查找的值
     * @return
     */
    public static int getIndex(int[] arr, int left, int right, int value) {

        // 说明没有找到
        if (left > right) {
            return -1;
        }

        int mid = (left + right) / 2;
        int midValue = arr[mid];

        if (value > midValue) {
            // 向右递归
            return getIndex(arr, mid + 1, right, value);
        } else if (value < midValue) {
            // 向左递归
            return getIndex(arr, left, mid - 1, value);
        } else {
            return mid;
        }
    }

    /**
     * 查找的元素可能包含多个, 注意这是一个有序数组，所以相同的元素就在一块
     *
     * @param arr   原始数组
     * @param left  初始索引
     * @param right 数组尾部索引
     * @param value 查找的值
     * @return
     */
    public static List<Integer> getData(int[] arr, int left, int right, int value) {
        if (left > right) {
            // 返回一个空集合
            return new ArrayList<Integer>();
        }
        int mid = (left + right) / 2;
        int midValue = arr[mid];

        if (value > midValue) {
            // 向右递归
            return getData(arr, mid + 1, right, value);
        } else if (value < midValue) {
            // 向左递归
            return getData(arr, left, mid - 1, value);
        } else {

            // 用于存放数组的下标值
            List<Integer> list = new ArrayList<>();

            // 向mid索引值的左边扫描，将所有满足条件的元素下标存放到list集合中
            int temp = mid - 1;

            while (true) {
                if (temp < 0 || arr[temp] != value) {
                    break;
                }

                list.add(temp);
                temp -= 1;
            }

            list.add(mid);

            // 向右边扫描
            temp = mid + 1;
            while (true) {
                if (temp > arr.length - 1 || arr[temp] != value) {
                    break;
                }
                list.add(temp);
                temp += 1;
            }
            return list;
        }

    }
}
