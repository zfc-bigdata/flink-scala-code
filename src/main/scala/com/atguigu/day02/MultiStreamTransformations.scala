package com.atguigu.day02

import com.atguigu.day03.SmokeLevelSource
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object MultiStreamTransformations {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val keyedTempStream =
      env
        .addSource(new SensorSourceII)
        .keyBy(r=>r.id)

    val smokeLevelStream =
      env
        .addSource(new SmokeLevelSource)
        .setParallelism(1)

    keyedTempStream
        .connect(smokeLevelStream.broadcast)
        .flatMap(new RaiseAlertFlatMap)
        .print()
    env.execute()
  }
  class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReadingII,String,Alert]{
    private var smokeLevel : String = "LOW"

    override def flatMap1(value: SensorReadingII, out: Collector[Alert]): Unit = {
      if (smokeLevel == "HIGH" && value.temperature > -100.0) {
        out.collect(Alert(value.toString,value.timestamp))
      }
    }

    override def flatMap2(value: String, out: Collector[Alert]): Unit = {
      smokeLevel = value
    }
  }
}
