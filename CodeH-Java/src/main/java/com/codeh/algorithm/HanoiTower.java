package com.codeh.algorithm;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className HanoiTower
 * @date 2021/4/15 18:50
 * @description 汉洛塔实现
 */
public class HanoiTower {
    public static void main(String[] args) {
        Tower tower = new Tower();
        tower.move(64, 'A', 'B', 'C');
    }
}

class Tower {

    /**
     * @param num 移动的数量
     * @param a   塔
     * @param b   塔
     * @param c   塔
     */
    public void move(int num, char a, char b, char c) {
        if (num == 1) {
            System.out.println(a + "->" + c);
        } else {
            // 先移动上面所有盘到b，借助c
            move(num - 1, a, c, b);

            // 把最下面的盘移动到c
            System.out.println(a + "->" + c);

            // 再把b塔所有盘移动到c，借助于a
            move(num - 1, b, a, c);
        }
    }
}
