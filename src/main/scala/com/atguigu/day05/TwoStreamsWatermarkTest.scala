package com.atguigu.day05

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoStreamsWatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream1 = env
      .socketTextStream("localhost", 9999, '\n')
      .map(r => {
        val arr = r.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[(String, Long)](Duration.ofMillis(0))
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(t: (String, Long), l: Long): Long = {
              t._2
            }
          })
      )
    val stream2 = env
      .socketTextStream("localhost", 9998, '\n')
      .map(r => {
        val arr = r.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[(String, Long)](Duration.ofMillis(0))
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(t: (String, Long), l: Long): Long = {
              t._2
            }
          })
      )
    stream1.union(stream2)
      .keyBy(r=>r._1)
      .process(new Pro)
      .print

    env.execute()

  }
  class Pro extends KeyedProcessFunction[String,(String,Long),String]{
    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), String]#Context, collector: Collector[String]): Unit = {
      collector.collect("时间戳为 : " + context.timerService().currentProcessingTime()
      +" 的水位线来了")
    }
  }
}
