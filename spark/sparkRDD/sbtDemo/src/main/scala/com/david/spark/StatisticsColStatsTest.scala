package com.david.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

//以列为基础计算  基本统计量
object StatisticsColStatsTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName()).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd =sc.parallelize(Seq(
      Vectors.dense(1.0, 10.0, 100.0),
      Vectors.dense(2.0, 20.0, 200.0),
      Vectors.dense(3.0, 30.0, 300.0)
    ))  //Seq from collection
    val summary = Statistics.colStats(rdd)
    println(summary.max)
    println(summary.min)
    println("count" + summary.count)
    println(summary.numNonzeros) //非零
    println("variance:" + summary.variance) //方差
    println(summary.mean) //计算均值
    println(summary.variance) //计算标准差
    println(summary.normL1) //计算曼哈段距离:相加
    println(summary.normL2) //计算欧几里得距离：平方根
  }
}
