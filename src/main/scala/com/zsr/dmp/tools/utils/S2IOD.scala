package com.zsr.dmp.tools.utils

/**
  * @author: zhouyi
  * @create: 2019-06-11 22:58:46
  **/
object S2IOD {
  // 转换Int
  def toInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case _:Exception =>0
    }
  }
  // 转换Double
  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case _:Exception =>0
    }
  }
}
