package com.codeh.stream.transform

import com.codeh.stream.source.SensorReading
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * @className Transform
 * @author jinhua.xu
 * @date 2021/4/28 16:43
 * @description 转换算子，对数据进行转换操作，常用算子map，flatMap，Filter，keyBy，minBy，min，Reduce，Split，Select，Connect，CoMap，Union等
 * @version 1.0
 */
object Transform {
  def main(args: Array[String]): Unit = {
    // 1.创建环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val path: String = "D:\\IDEA_Project\\flink_scala\\src\\main\\resources\\sensor.txt"
    val ds: DataStream[String] = env.readTextFile(path)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    /**
     * 4.1 minBy与min的区别
     * minBy：输出的是最小值的那一行数据
     * min：输出的还是第一条读取的数据，只是将比较的字段取了最小值
     *
     * keyBy里面的参数可以使用下标，字段名称
     */
    //    val min_res: DataStream[SensorReading] = dataStream.keyBy("id")
    //      .minBy("temperature")
    //
    //    min_res.print()


    // 4.2 Reduce使用. 取出最新的时间，温度取最小值
    //    val reduceStream: DataStream[SensorReading] = dataStream.keyBy("id") // 对id进行分组
    //      .reduce((stateReading, newReading) => SensorReading(stateReading.id, newReading.timestamp, stateReading.temperature.min(newReading.temperature)))
    //
    //    reduceStream.print()

    // 4.3 分流 Split 按照温度，将流拆分为高温流，低温流
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 35) Seq("high") else Seq("low")
    })

    val highStream: DataStream[SensorReading] = splitStream.select("high")
    val lowStream: DataStream[SensorReading] = splitStream.select("low")
    val allStream: DataStream[SensorReading] = splitStream.select("high","low")

    //    highStream.print("high")
    //    lowStream.print("low")
    //    allStream.print("all")

    // 4.4 合流 connect 将两个流合并成一个流，流的数据类型可以不一致
    /**
     * connect与union的区别：
     *  1.union之前两个流的类型必须是一样的，Connect可以不一样，在之后的coMap中再去调整成为一样的
     *  2.Connect只能操作两个流，Union可以操作多个
     */
    val highConnectStream: DataStream[(String, Double)] = highStream.map(data => (data.id, data.temperature))
    val connect: ConnectedStreams[(String, Double),SensorReading] = highConnectStream.connect(lowStream)

    val connectStream: DataStream[Product with Serializable] = connect.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, lowData.temperature, "healthy", lowData.timestamp)
    )

    connectStream.print("connectStream")


    env.execute("transform_test")

  }
}

// 富函数，可以获取运行时上下文，还有一些生命周期方法
class MyRichMapFunction extends RichMapFunction[SensorReading, String] {

  override def open(parameters: Configuration): Unit = {
    // 做一些初始化的操作，比如数据库的连接
    val subtask = getRuntimeContext.getIndexOfThisSubtask
  }

  override def map(value: SensorReading): String = value.id

  override def close(): Unit = {
    // 一般做收尾工作，比如关闭数据库连接，清空状态
  }
}

