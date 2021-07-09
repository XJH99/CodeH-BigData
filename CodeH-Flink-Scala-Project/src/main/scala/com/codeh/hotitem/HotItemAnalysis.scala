package com.codeh.hotitem

import java.sql.Timestamp
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @className HotItemAnalysis
 * @author jinhua.xu
 * @date 2021/5/8 11:40
 * @description 热门商品分析：每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品
 * @version 1.0
 */
object HotItemAnalysis {
  def main(args: Array[String]): Unit = {
    // 1.创建流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据并将数据进行初步转化
    val userBehavior: DataStream[UserBehavior] = env.readTextFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val lineArray: Array[String] = line.split(",")
        UserBehavior(lineArray(0).toLong, lineArray(1).toLong, lineArray(2).toInt, lineArray(3), lineArray(4).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000) //由于数据的时间是有序的，所以使用assignAscendingTimestamps，真实业务数据是乱序的

    // 3.过滤出点击数据
    val filterData: DataStream[UserBehavior] = userBehavior.filter(_.behavior == "pv")

    // 4.时间窗口内对数据进行聚合操作
    filterData.keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5)) //使用滑动窗口来实现功能
      .aggregate(new CountAgg(), new WindowResultFunction()) // 增量聚合操作
      .keyBy("windowEnd") //对窗口进行分组
      .process(new TopNHotItem(3)) // 获取点击前三的产品
      .print()

    env.execute("HotItemAnalysis")
  }
}

// count统计的聚合函数实现，每出现一条记录加一
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

/**
 * 将每个key，每个窗口聚合后的结果带上其它信息进行输出
 *
 * WindowFunction[IN, OUT, KEY, W <: Window]
 *
 * IN: 输入的类型
 * OUT：输出的结果
 * KEY： 传入的键
 * W: 使用的窗口类型
 */
class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    // 获取分组的key值
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    // 获取统计的数量
    val count: Long = input.iterator.next()
    // 输出每个商品在每个窗口的点击量的数据流
    out.collect(ItemViewCount(itemId, window.getEnd, count))
  }
}

/**
 * 求某个窗口中前N名的热门点击商品，key为窗口时间戳，输出为TopN的结果字符串
 *
 * @param i
 */
class TopNHotItem(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  // 定义一个状态变量
  lazy val itemState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount]))

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 1.将每条数据都保存到状态中
    itemState.add(value)

    // 2.注册windowEnd + 1的定时器，当触发时说明收齐了属于windowEnd窗口的所有商品数据
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1) // 加上1ms后表示之前的数据都到齐了
  }

  // 定时器触发，表示所有窗口统计结果已到齐，可以排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 方便排序，定义一个ListBuffer，保存ListState中的数据
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()

    // 获取迭代器
    val iter: util.Iterator[ItemViewCount] = itemState.get().iterator()

    while (iter.hasNext) {
      allItems += iter.next()
    }

    // 清空状态,释放空间
    itemState.clear()

    // 按照点击量从大到小排序
    val sortItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count).reverse.take(topSize)

    // 将排名信息格式化为String，便于打印数据
    val result: StringBuilder = new StringBuilder

    result.append("================================\n")
    result.append("时间： ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortItems.indices) {
      // 获取商品信息
      val currentItem: ItemViewCount = sortItems(i)

      result.append("NO").append(i + 1).append(":")
        .append(" 商品 ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count).append("\n")
    }

    result.append("================================\n\n")

    // 控制输出的频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}


// 定义用户行为日志数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 输出结果的样例类: 商品，窗口编号，数量
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
