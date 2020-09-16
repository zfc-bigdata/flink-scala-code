package com.atguigu.day05

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputExample {
  val output1 = new OutputTag[String]("side-output")
  val output2 = new OutputTag[String]("side-output")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)

    val warnings = stream.process(new FreezingAlarm)
    warnings.getSideOutput(output1).print()
    warnings.getSideOutput(output2).print()

    env.execute()


  }
  class FreezingAlarm extends ProcessFunction[SensorReading,SensorReading]{
    override def processElement(value: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        ctx.output(output1,"传感器ID为: " + value.id + "的传感器温度小于32度")
      }
      if (value.temperature < 22.0) {
        ctx.output(output2,"传感器ID为: " + value.id + "的传感器温度小于22度")
      }
      out.collect(value)
    }
  }
}
