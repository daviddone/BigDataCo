package com.david.spark.mlib.regression
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

//线性相关 小数据量
object LinearRegression {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    val data = sc.textFile("D:\\david_study\\源码阅读\\github相关\\BigDataCo\\spark\\sparkRDD\\sbtDemo\\data\\mlib\\lineRe.data")							//获取数据集路径
    val parsedData = data.map { line => //开始对数据集处理
      val parts = line.split(',') //根据逗号进行分区
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache() //转化数据格式
    val model = LinearRegressionWithSGD.train(parsedData, 100, 0.1) //建立模型
    val result = model.predict(Vectors.dense(2,1)) //通过模型预测模型
    println("model weights:")
    println(model.weights)
    println("result:")
    println(result) //打印预测结果
    sc.stop
  }
}
