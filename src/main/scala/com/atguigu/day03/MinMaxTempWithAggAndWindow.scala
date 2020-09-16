package com.atguigu.day03

import java.sql.Timestamp

import com.atguigu.day02.{SensorReadingII, SensorSourceII}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.aggregation.AggregationFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempWithAggAndWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1);
    val stream = env.addSource(new SensorSourceII).filter(r => r.id.equals("sensor_1"))

    stream
      .keyBy(r=>r.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new Agg,new Win)
      .print()

    env.execute()

  }
  class Agg extends AggregateFunction[SensorReadingII,(Double,Double),(Double,Double)]{
    override def createAccumulator(): (Double, Double) = {
      (Double.MaxValue,Double.MinValue)

    }


    override def add(in: SensorReadingII, acc: (Double, Double)): (Double, Double) = {
      (acc._1.min(in.temperature),acc._2.max(in.temperature))
    }

    override def getResult(acc: (Double, Double)): (Double, Double) = {
      acc
    }

    override def merge(acc: (Double, Double), acc1: (Double, Double)): (Double, Double) = ???
  }
  class Win extends ProcessWindowFunction[(Double,Double),MinMaxTemp,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val e = elements.head
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd
      out.collect(MinMaxTemp(key,e._1,e._2,
        "窗口: "+new Timestamp(windowStart)+"~"+new Timestamp(windowEnd)))

    }
  }
  case class MinMaxTemp(id:String,minTemp:Double,maxTemp:Double,window:String)

}
