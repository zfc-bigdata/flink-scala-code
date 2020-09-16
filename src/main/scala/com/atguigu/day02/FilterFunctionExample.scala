package com.atguigu.day02

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1);

    val stream = env.addSource(new SensorSourceII)

    stream.filter(r=>r.temperature > 0.0)

    stream.filter(new MyFilterFunction)

    stream.filter(new FilterFunction[SensorReadingII] {
      override def filter(value: SensorReadingII): Boolean = value.temperature>0.0
    })
      .print()
    env.execute()
  }
    class MyFilterFunction extends FilterFunction[SensorReadingII]{
      override def filter(value: SensorReadingII): Boolean = value.temperature >0.0
    }
}
