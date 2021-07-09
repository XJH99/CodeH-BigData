package com.codeh.stream.state

import java.util

import com.codeh.stream.source.SensorReading
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @className StateProgram
 * @author jinhua.xu
 * @date 2021/4/28 18:25
 * @description 状态编程
 * @version 1.0
 */
object StateProgram {
  def main(args: Array[String]): Unit = {
    // 1.创建环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val ds: DataStream[String] = env.socketTextStream("192.168.214.136", 9999)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    // 需求：对于温度传感器温度值跳变，超过10度，报警
    val res: DataStream[(String, Double, Double)] = dataStream
      .keyBy("id")
      //.flatMap(new TempWarning(10.0))
      .flatMapWithState[(String, Double, Double), Double] {
        case (data1: SensorReading, None) => (List.empty, Some(data1.temperature))
        case (data2: SensorReading, lastTemp: Some[Double]) =>
          // 得到两次温度的绝对差值
          val diff = (lastTemp.get - data2.temperature).abs

          // 判断差值与传入值的差距
          if (diff > 10.0)
          // 输出数据
            (List((data2.id, lastTemp.get, data2.temperature)), Some(data2.temperature))
          else (List.empty, Some(data2.temperature))
      }

    res.print()

    env.execute("state test")
  }
}


// 实现自定义RichFlatMapFunction
class TempWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  // 定义一个保存上一个温度数据的状态值
  lazy val tempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("tempState", classOf[Double]))

  // 定义一个标志状态值
  lazy val flag: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("flag", classOf[Boolean]))

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上一次的温度值
    val tempValue: Double = tempState.value()

    // 获取当前状态值
    val tempFlag: Boolean = flag.value()

    // 得到两次温度的绝对差值
    val diff = (tempValue - value.temperature).abs

    // 判断差值与传入值的差距
    if (diff > threshold) {
      // 输出数据
      out.collect((value.id, tempValue, value.temperature))
    }

    // 对温度状态值进行更新
    tempState.update(value.temperature)
  }
}


// keyed state测试：必须定义在富函数中，因为需要运行时上下文
class MyRichMap1 extends RichMapFunction[SensorReading, String] {

  var valueState: ValueState[Double] = _

  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState", classOf[Int]))
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double]))
  // lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reduceState", new MyReduce(), classOf[SensorReading]))

  // 要声明在open生命周期内
  override def open(parameters: Configuration): Unit = {
    // 键控状态的定义
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
  }

  override def map(value: SensorReading): String = {
    // 状态的读写
    val myValue: Double = valueState.value()

    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)
    listState.update(list)

    mapState.contains("sensor_1")
    mapState.get("sensor_2")
    value.id
  }
}

