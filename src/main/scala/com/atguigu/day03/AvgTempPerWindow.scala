package com.atguigu.day03


import com.atguigu.day02.{SensorReadingII, SensorSourceII}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AvgTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1);

    val stream = env.addSource(new SensorSourceII)
    stream.keyBy(r=>r.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTemp)
      .print()
  }
  class AvgTemp extends AggregateFunction[SensorReadingII,(String,Double,Long),(String,Double)]{
    override def createAccumulator(): (String, Double, Long) = {

      ("",0.0,0L)
    }

    override def add(in: SensorReadingII, acc: (String, Double, Long)): (String, Double, Long) = {
      (in.id,acc._2+in.temperature,acc._3+1)
    }

    override def getResult(acc: (String, Double, Long)): (String, Double) = {

      (acc._1,acc._2/acc._3)
    }

    override def merge(acc: (String, Double, Long), acc1: (String, Double, Long)): (String, Double, Long) = ???
  }
}
