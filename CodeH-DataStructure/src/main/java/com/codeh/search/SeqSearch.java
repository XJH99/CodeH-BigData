package com.codeh.search;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SeqSearch
 * @date 2021/6/28 13:19
 * @description 线性查找算法
 */
public class SeqSearch {
    public static void main(String[] args) {
        int[] arr = {1, 9, 11, -1, 89};
        int index = getDataIndex(arr, 89);

        if (index == -1) {
            System.out.println("没有找到该元素");
        } else {
            System.out.println("找到了，该元素的下标为：" + index);
        }

    }

    /**
     * @param arr   原始数组
     * @param value 查找的元素
     * @return
     */
    public static int getDataIndex(int[] arr, int value) {
        int index = -1;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == value) {
                index = i;
            }
        }
        return index;
    }
}
