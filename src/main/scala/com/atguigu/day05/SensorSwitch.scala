package com.atguigu.day05

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SensorSwitch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource).keyBy(r => r.id)
    val switchs = env.fromElements(("sensor_1", 10 * 1000L)).keyBy(r=>r._1)

    stream
      .connect(switchs)
      .process(new SwitchProcess)
      .print()

    env.execute()
  }
  class SwitchProcess extends CoProcessFunction[SensorReading,(String,Long),SensorReading]{
    lazy val forwardSwitch = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("switch",Types.of[Boolean])
    )
    override def processElement1(in1: SensorReading, context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      if(forwardSwitch.value()){
        collector.collect(in1)
      }
    }

    override def processElement2(in2: (String, Long), context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      forwardSwitch.update(true)
      context.timerService().registerProcessingTimeTimer(
        context.timerService().currentProcessingTime() + in2._2)
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {

      forwardSwitch.clear()
    }
  }
}
