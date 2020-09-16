package com.atguigu.day02

case class Alert(message:String,timeStamp:String){
  override def toString: String = "出事儿啦!!" + message + "着火了!!"
}
