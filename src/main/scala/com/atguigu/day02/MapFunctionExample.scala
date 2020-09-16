package com.atguigu.day02

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSourceII)
    stream.map(r=>r.id)
    stream.map(new IdExtractor)
    stream.map(new MapFunction[SensorReadingII,String] {
      override def map(t: SensorReadingII): String = t.id
    })
      .print()
    env.execute()
  }
class IdExtractor extends MapFunction[SensorReadingII,String]{
  override def map(value: SensorReadingII): String = value.id
}
}
