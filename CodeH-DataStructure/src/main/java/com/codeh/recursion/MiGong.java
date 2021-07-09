package com.codeh.recursion;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className MiGong
 * @date 2021/6/16 15:07
 * @description 使用二维数组解决迷宫问题
 */
public class MiGong {
    public static void main(String[] args) {

        // 1.先创建一个二维数组,模拟迷宫
        int[][] map = new int[8][7];

        // 2.上下两边的顶端置为1
        for (int i = 0; i < 7; i++) {
            map[0][i] = 1;
            map[7][i] = 1;
        }

        // 3.左右两边都置为1
        for (int i = 0; i < 8; i++) {
            map[i][0] = 1;
            map[i][6] = 1;
        }

        map[3][1] = 1;
        map[3][2] = 1;


        System.out.println("迷宫盘如下~~");
        for (int i = 0; i < map.length; i++) {
            for (int j = 0; j < map[i].length; j++) {
                System.out.printf("%d\t", map[i][j]);
            }
            System.out.println("");
        }

        setWay(map,3,3);

        System.out.println("小球走过的迷宫盘如下~~");
        for (int i = 0; i < map.length; i++) {
            for (int j = 0; j < map[i].length; j++) {
                System.out.printf("%d\t", map[i][j]);
            }
            System.out.println("");
        }
    }

    /**
     * 使用递归回溯来给小球找路
     * 当map[i][j]为0表示该点没有走过，当为1表示墙；2表示通路可以走；3表示该点已走过但是走不通
     * 行走时的策略 下 -》右 -》上 -》左
     *
     * @param map 表示地图
     * @param i   迷宫的行
     * @param j   迷宫的列
     * @return 如果找到通路返回为true，否则返回false
     */
    public static boolean setWay(int[][] map, int i, int j) {
        if (map[6][5] == 2) {
            return true;
        } else {
            if (map[i][j] == 0) { // 表示这个点没走过，可以走
                map[i][j] = 2;
                if (setWay(map, i + 1, j)) {
                    return true;
                } else if (setWay(map, i, j + 1)) {
                    return true;
                } else if (setWay(map, i - 1, j)) {
                    return true;
                } else if (setWay(map, i, j - 1)) {
                    return true;
                } else {    // 说明该点走不通，是死路
                    map[i][j] = 3;
                    return false;
                }
            } else {
                return false;
            }
        }

    }
}
