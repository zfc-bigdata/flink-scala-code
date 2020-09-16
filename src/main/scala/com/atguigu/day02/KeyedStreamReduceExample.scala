package com.atguigu.day02

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

object KeyedStreamReduceExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSourceII).filter(r => r.id.equals("sensor_1"))

    stream.map(r => (r.id,r.temperature))
      .keyBy(r =>r._1)
      .reduce((r1,r2)=>(r1._1,r1._2.max(r2._2)))

    stream.map(r=>(r.id,r.temperature))
      .keyBy(r=>r._1)
      .reduce(new MyReduceFunction)
      .print()
  }
  class MyReduceFunction extends ReduceFunction[(String,Double)]{
    override def reduce(t: (String, Double), t1: (String, Double)): (String, Double) = (t._1,t._2.max(t1._2))
  }
}
