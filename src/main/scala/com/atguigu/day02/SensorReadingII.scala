package com.atguigu.day02

case class SensorReadingII(
                        id:String,
                        timestamp:String,
                        temperature:Double
                        ){
  override def toString: String = "节点: " + id + "时间: " + timestamp + "温度: " + temperature
}
