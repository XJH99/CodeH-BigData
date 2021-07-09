package com.codeh.recursion;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Queue8
 * @date 2021/6/22 14:13
 * @description 使用回溯算法解决八皇后问题:任意两个皇后都不能处于同一列或同一斜线上，问有多少种摆法（92）
 */
public class Queue8 {

    int max = 8;
    int[] arr = new int[max];
    static int count = 0;

    public static void main(String[] args) {
        Queue8 queue8 = new Queue8();
        queue8.check(0);
        System.out.printf("一共有%d种解法", count);

    }

    /**
     * 放置第 n 个皇后
     *
     * @param n
     */
    public void check(int n) {
        if (n == max) { // n = 8表示前面的八个皇后都放好了
            print();
            return;
        }

        // 依次放入皇后，并判断是否冲突
        for (int i = 0; i < max; i++) {
            // 先把当前这个皇后n，放在该行的第一列
            arr[n] = i;

            if (judge(n)) {
                // 不冲突，放置 n + 1个皇后
                check(n + 1);
            }
            // 冲突，就将继续执行 arr[n] = i; 将第n个皇后放在本行后移一个的位置
        }

    }

    /**
     * 查看当前放置第 n 个皇后，就去检查当前放置的皇后是否与前面放置的皇后冲突
     *
     * @param n 表示第 n 个皇后
     * @return
     */
    public boolean judge(int n) {
        for (int i = 0; i < n; i++) {
            // arr[i] == arr[n] 表示判断两个皇后是否在同一列上
            // Math.abs(n-i) == Math.abs(arr[n] - arr[i]) 表示判断第n个皇后与第i个皇后是否在同一斜线上
            if (arr[i] == arr[n] || Math.abs(n - i) == Math.abs(arr[n] - arr[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * 将皇后的位置打印出来
     */
    public void print() {
        count ++;
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + " ");
        }
        System.out.println();
    }
}
