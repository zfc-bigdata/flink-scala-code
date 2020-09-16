package com.atguigu.day02

import org.apache.flink.streaming.api.scala._

object KeyedStreamExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSourceII).filter(r => r.id.equals("sensor_1"))

    val keyedStream = stream.keyBy(r => r.id)

    val maxStream = keyedStream.max(2)
    maxStream.print()
    env.execute()
  }
}
