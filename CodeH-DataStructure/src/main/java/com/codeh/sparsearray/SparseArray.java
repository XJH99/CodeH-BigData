package com.codeh.sparsearray;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SparseArray
 * @date 2021/5/31 22:28
 * @description 二维数组转稀疏数组，稀疏数组转二维数组
 */
public class SparseArray {
    public static void main(String[] args) {
        // 1.创建一个空的二维数组
        int arr1[][] = new int[11][11];

        // 2.给对应的一些位置赋值
        arr1[1][2] = 1;
        arr1[3][5] = 20;
        arr1[4][6] = 18;
        arr1[9][5] = 31;

        // 3.遍历二维数组
        for (int tmp[] : arr1) {
            for (int data : tmp) {
                System.out.printf("%d\t", data);
            }
            System.out.println();
        }

        // 4.将二维数组转为稀疏数组
        int sum = 0;
        for (int tmp[] : arr1) {
            for (int data : tmp) {
                // 4.1如果值不等于0，元素个数加一
                if (data != 0) {
                    sum++;
                }
            }
        }

        // 5.创建稀疏数组
        int sparse[][] = new int[sum + 1][3];

        // 6.给稀疏数组进行赋值
        sparse[0][0] = 11;
        sparse[0][1] = 11;
        sparse[0][2] = sum;

        // 7.遍历二维数组，将非0值存放到稀疏数组中
        int count = 0;
        for (int i = 0; i < 11; i++) {
            for (int j = 0; j < 11; j++) {
                if (arr1[i][j] != 0) {
                    count++;
                    sparse[count][0] = i;
                    sparse[count][1] = j;
                    sparse[count][2] = arr1[i][j];
                }
            }
        }

        // 8.遍历稀疏数组
        System.out.println("----------稀疏数组---------");

        for (int tmp[] : sparse) {
            for (int data : tmp) {
                System.out.printf("%d\t", data);
            }
            System.out.println();
        }

        // 9.稀疏数组还原成二维数组
        int arr2[][] = new int[sparse[0][0]][sparse[0][1]];

        // 10.读取稀疏数组的值，将其赋值给二维数组
        for (int i= 1; i< sparse.length; i++) {
            arr2[sparse[i][0]][sparse[i][1]] = sparse[i][2];
        }

        // 11.输出转换后的二维数组
        System.out.println("---------还原后的二维数组----------");
        for (int tmp[]: arr2) {
            for (int data: tmp) {
                System.out.printf("%d\t", data);
            }
            System.out.println();
        }
    }
}
