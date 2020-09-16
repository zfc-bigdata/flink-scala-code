package com.atguigu.day03



import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random


class SmokeLevelSource extends RichParallelSourceFunction[String]{
  private var running : Boolean = true
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    val random = new Random
    while (running){
      if(random.nextGaussian() > 0.8){
        sourceContext.collect("HIGH")
      }else{
        sourceContext.collect("LOW")
      }
    }
  }

  override def cancel(): Unit = running = false
}
