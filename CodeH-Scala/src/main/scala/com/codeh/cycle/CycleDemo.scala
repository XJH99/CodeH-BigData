package com.codeh.cycle

/**
 * @className CycleDemo
 * @author jinhua.xu
 * @date 2021/8/27 12:12
 * @description for循环的使用案例
 * @version 1.0
 */
object CycleDemo {
  def main(args: Array[String]): Unit = {

    // to是左闭右闭合
    for (i <- 1 to 10) {
      print(i + "\t") // 1	2	3	4	5	6	7	8	9	10
    }

    println()

    // until是左闭右开
    for (k <- 1 until 10) {
      print(k + "\t") // 1	2	3	4	5	6	7	8	9
    }

    println()

    // 类似于双重for循环
//    for (m <- 1 to 10; n <- 1 to 3) {
//      println(m + "--" + n)
//    }

    val numList = List(1,2,3,4);
    for (p <- numList; if p < 3) {  // 循环守卫
      print(p + "\t")
    }

    println()

  }

}
