package com.codeh.function

/**
 * @className Test
 * @author jinhua.xu
 * @date 2021/8/27 14:19
 * @description 1.指定函数参数名 2.
 * @version 1.0
 */
object Test {
  def main(args: Array[String]): Unit = {
    //printInt(b=10, a=20)
    //printStrings("tom", "jack", "cary")
    //println(recursion(10))
    //defaultParas()
    //println(mulCurry(10)(20))

    val f = minus(10)
    println("f1=" + f(1))
  }

  def printInt(a: Int, b: Int) = { // 1.可以指定函数参数名
    println(a)
    println(b)
  }

  def printStrings(args: String*): Unit = { // 2.函数可以指定可变参数
    for (m <- args) {
      println("Arg value = " + m)
    }
  }

  def recursion(n: BigInt): BigInt = { // 3.递归函数
    if (n == 1) {
      n
    } else {
      n * recursion(n - 1)
    }
  }

  def defaultParas(a: Int = 5, b: Int = 10): Unit = { // 4.默认参数值
    println(a + b)
  }

  /**
   * 偏函数的引入：是和函数平行的一种概念，scala中的偏函数是一个trait，类型为PartialFunction[A,B],接收A的参数返回B的结果
   * PartialFunction[Any,Int] 表示偏函数接收的参数类型为Any，返回类型是Int
   * isDefinedAt(x: Any) 如果返回值是true才会执行下面apply函数
   * apply构造器，对传入的值 + 1，并返回新的集合
   *
   * 如果使用偏函数，则不能使用map，应该使用collect
   */
  val res: PartialFunction[Any, Int] = new PartialFunction[Any, Int] { // 5.偏函数使用
    override def isDefinedAt(x: Any): Boolean = x.isInstanceOf[Int]

    override def apply(v1: Any): Int = v1.asInstanceOf[Int] + 1
  }

  def mulCurry(x: Int)(y: Int) = x * y // 6.函数柯里化：可以将接收多个参数的函数转化为接收单个参数的函数，这个转化的过程就是柯里化

  def minus(x: Int) = (y: Int) => x - y // 7.闭包就是一个函数和与其相关的引用环境组合的一个整体，返回的是一个匿名函数 ，因为该函数引用到到函数外的 x,那么该函数和x整体形成一个闭包



}
