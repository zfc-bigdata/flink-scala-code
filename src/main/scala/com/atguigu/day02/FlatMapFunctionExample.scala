package com.atguigu.day02

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("white", "black", "grey")
    stream.flatMap(new MyFilterMapFunction).print()

    stream.flatMap(new FlatMapFunction[String,String] {
      override def flatMap(value: String, out: Collector[String]): Unit = {
        if (value == "white") {
          out.collect(value)
        }else if(value == "black"){
          out.collect(value)
          out.collect(value)
        }

      }
    })
    env.execute()
  }
  class MyFilterMapFunction extends FlatMapFunction[String,String]{
    override def flatMap(value: String, out: Collector[String]): Unit = {
      if (value == "white") {
        out.collect(value)
      }else if(value == "black"){
        out.collect(value)
        out.collect(value)
      }
    }
  }
}
