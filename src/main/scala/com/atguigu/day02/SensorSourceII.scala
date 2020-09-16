package com.atguigu.day02

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source._

import scala.util.Random

class SensorSourceII extends RichParallelSourceFunction[SensorReadingII]{
  var running = true
  override def run(ctx: SourceContext[SensorReadingII]): Unit = {
    val rand = new Random

    var curFTemp = (1 to 10).map(i=>("sensor_"+i,rand.nextGaussian()*20))

    while (running){
      curFTemp.map(t =>(t._1,t._2+(rand.nextGaussian()*0.5)))

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      val curTime = format.format(Calendar.getInstance.getTimeInMillis).toString

      curFTemp.foreach(t=>ctx.collect(SensorReadingII(t._1,curTime,t._2)))
      Thread.sleep(100)
    }

  }

  override def cancel(): Unit = running = false
}
