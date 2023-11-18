package com.codeh.jvm;

/**
 * @author jinhua.xu
 * @version 1.0
 * @date 2023/4/2 15:50
 * @describe JVM堆内存参数调整 -Xms1024m -Xmx1024m -XX:+PrintGCDetails
 */
public class HeapTest {

  public static void main(String[] args) {
    // 返回jvm初始化总内存
    long totalMemory = Runtime.getRuntime().totalMemory();

    // 返回虚拟机试图使用的最大内存
    long maxMemory = Runtime.getRuntime().maxMemory();

    System.out.println("total:" + totalMemory + "字节\t" + (totalMemory/(double)1024/1024) + "MB");
    System.out.println("max:" + maxMemory + "字节\t" + (maxMemory/(double)1024/1024) + "MB");
  }
}
