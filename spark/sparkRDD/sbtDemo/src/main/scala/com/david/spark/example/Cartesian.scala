package com.david.spark.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Cartesian {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Cartesian").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val d = sc.parallelize(1 to 100, 10)
    d.foreach(println)
  }
}