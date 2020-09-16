package com.atguigu.day04

import com.atguigu.day02.{SensorReading, SensorReadingII, SensorSource, SensorSourceII}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    //创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度 1
    env.setParallelism(1)
    //添加数据源 生成SensorReading数据
    val stream = env.addSource(new SensorSource)
    //按K分组 将具有同一关键字的元素 分为一组 为后面分组聚合 规约 做准备
    stream.keyBy(r => r.id)
      //全量聚合
      .process(new TempIncrease)
      //打印
      .print
    //执行算子
    env.execute()

  }
  //创建KeyedProcessFunction 子类 以供process调用
  class TempIncrease extends KeyedProcessFunction[String,SensorReading,String]{
    //创建懒加载变量即 有状态变量
    lazy val lastTemp : ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp",Types.of[Double])
    )

    lazy val timer : ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer",Types.of[Long])
    )

    override def processElement(value : SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out : Collector[String]): Unit = {
      val prevTemp = lastTemp.value()
      lastTemp.update(value.temperature)
      val ts = timer.value()

      if (prevTemp == 0.0 || value.temperature < prevTemp) {
        ctx.timerService().deleteEventTimeTimer(ts)
        timer.clear()

      }else if(value.temperature > prevTemp && ts == 0){
        val oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L
        ctx.timerService().registerProcessingTimeTimer(oneSecondLater)
        timer.update(oneSecondLater)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

      out.collect("传感器ID是: "+ctx.getCurrentKey + " 的传感器的温度连续1s上升了!!")
      timer.clear()
    }
  }
}
