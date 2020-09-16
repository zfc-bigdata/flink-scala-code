package com.atguigu.day03

import java.sql.Timestamp

import com.atguigu.day02.{SensorReadingII, SensorSourceII}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1);

    val stream = env.addSource(new SensorSourceII)
      .filter(r => r.id.equals("sensor_1"))

    stream
      .keyBy(r=>r.id)
      .timeWindow(Time.seconds(5))
      .process(new MinMaxProcessFunction)
      .print()

    env.execute()
  }
  case class MinMaxTemp(id:String,minTemp:Double,maxTemp:Double,window:String)

  class MinMaxProcessFunction extends ProcessWindowFunction[SensorReadingII,
    MinMaxTemp,String,TimeWindow]{

    override def process(key: String,
                         context: Context, elements: Iterable[SensorReadingII],
                         out: Collector[MinMaxTemp]): Unit = {
      val temps = elements.map(_.temperature)
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd

      out.collect(MinMaxTemp(key,temps.min,temps.max,
        "窗口: "+ new Timestamp(windowStart)+"~"+new Timestamp(windowEnd)))
    }
  }
}
