package com.atguigu.day04


import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(60 * 1000L)

    val stream = env.socketTextStream("localhost", 9999, '\n')
    stream
      .map(r=>{
      val arr = r.split(" ")

      (arr(0),arr(1).toLong*1000L)
    })
      .assignTimestampsAndWatermarks(

        WatermarkStrategy
          .forBoundedOutOfOrderness[(String,Long)](Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(t: (String, Long), l: Long): Long = {
              t._2
            }
          })
      )
      .keyBy(r=>r._1)
      .timeWindow(Time.seconds(5))
      .process(new ProWin)
      .print()

    env.execute()
  }
  class ProWin extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {

      out.collect("窗口为: "+context.window.getStart + "----"+context.window.getEnd+
      "中有" + elements.size + "个元素")
    }
  }
}
