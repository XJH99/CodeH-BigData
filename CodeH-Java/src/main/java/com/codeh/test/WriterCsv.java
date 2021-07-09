package com.codeh.test;

import java.io.*;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className WriterCsv
 * @date 2021/4/20 13:40
 * @description 将数据写入到csv文件中
 */
public class WriterCsv {
    public static void main(String[] args) {

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Java\\test.csv")));

            for (int i = 0; i < 10; i++) {
                bw.write("hello" + i + "," + "Java" + "," + "scala");
                bw.newLine();
            }
            // 关闭资源
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
