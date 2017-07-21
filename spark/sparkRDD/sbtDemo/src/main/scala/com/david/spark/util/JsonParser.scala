package com.david.spark.util

import scala.io.Source
import scala.util.parsing.json.JSON

//解析json
object JsonParser {
  def regixJson(json:Option[Any]) =json match {
    case Some(map: Map[String, Any]) => map
  }
  def main(args: Array[String]): Unit = {
    parseJsonBasic
  }

  //采用scala自带JSON解析
  private def parseJsonBasic = {
    val jsonPath = "data/util/wechat.json"
    val jsonSource = Source.fromFile(jsonPath)
    val jsonStr = jsonSource.mkString
    println(jsonStr)

    val jsonInfo = JSON.parseFull(jsonStr)
    val jsonMap = regixJson(jsonInfo)
    println(jsonMap)
    val openid = jsonMap("openid")
    println("openid:" + openid)
  }
}
