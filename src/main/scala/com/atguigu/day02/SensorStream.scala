package com.atguigu.day02

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object SensorStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)

    stream.print()
    env.execute()

  }
}
