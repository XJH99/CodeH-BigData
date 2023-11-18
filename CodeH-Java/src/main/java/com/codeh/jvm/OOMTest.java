package com.codeh.jvm;

import java.util.Random;

/**
 * @author jinhua.xu
 * @version 1.0
 * @date 2023/4/2 16:02
 * @describe -Xms8m -Xmx8m -XX:+PrintGCDetails
 */
public class OOMTest {

  public static void main(String[] args) {
    String name = "ch";

    while (true) {
      name += name + new Random().nextInt(999999999) + new Random().nextInt(999999999);
    }
  }
}
