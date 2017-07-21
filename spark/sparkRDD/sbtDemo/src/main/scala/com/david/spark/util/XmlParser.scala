package com.david.spark.util

import scala.xml.XML
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.beans.BeanProperty
import scala.io.Source
//xml解析
object XmlParser {

  def main(arge: Array[String]) {
      val xmlPath = "D:\\david_study\\源码阅读\\github相关\\BigDataCo\\spark\\sparkRDD\\sbtDemo\\data\\util\\wechat.xml"
      val xmlInfo = Source.fromFile(xmlPath)
      xmlInfo.foreach{
        print
      }
      val xmlFile = XML.load("data/util/wechat.xml") // 中文路径乱码
      println(xmlFile)
      val fromUserName = (xmlFile\"FromUserName").text.toString.trim()
      println("fromUserName:"+fromUserName)
  }

}