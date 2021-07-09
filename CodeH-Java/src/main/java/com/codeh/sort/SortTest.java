package com.codeh.sort;

import org.junit.Test;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SortTest
 * @date 2021/4/15 17:21
 * @description 排序功能实现
 */
public class SortTest {

    int data[] = {24, 89, 30, 57, 13};
    int tmp = 0;

    /**
     * 冒泡排序：依次比较相邻元素的值，发现逆序则交换，较大的元素后移
     */
    @Test
    public void BubbleSort() {
        for (int i = 0; i < data.length - 1; i++) {
            for (int j = 0; j < data.length - 1 - i; j++) {
                if (data[j] > data[j + 1]) {
                    tmp = data[j];
                    data[j] = data[j + 1];
                    data[j + 1] = tmp;
                }
            }
        }
        for (int k = 0; k < data.length; k++) {
            System.out.print(data[k] + "\t");
        }
    }
}
