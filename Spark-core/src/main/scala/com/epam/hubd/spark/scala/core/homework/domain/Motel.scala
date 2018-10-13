package com.core.hubd.spark.scala.core.homework.domain

case class Motel(motelId: String, motelName: String){
  override def toString: String = s"$motelId,$motelName"
}
