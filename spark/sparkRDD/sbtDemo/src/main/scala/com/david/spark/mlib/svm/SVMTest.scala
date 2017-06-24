package com.david.spark.mlib.svm
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

// 支持向量机
object SVMTest {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))
    val sc = new SparkContext(conf)

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "D:\\david_study\\源码阅读\\github相关\\BigDataCo\\spark\\sparkRDD\\sbtDemo\\data\\mlib\\sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)
    println(model.weights)
    println("model.weights.size"+model.weights.size)
    scoreAndLabels.take(10).foreach(println)
    // Save and load model
    val iString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val path = "D:\\david_study\\源码阅读\\github相关\\BigDataCo\\spark\\sparkRDD\\sbtDemo\\data\\mlib\\sample_libsvm_data\\" + iString + "/result"
    model.save(sc, path)
    val sameModel = SVMModel.load(sc, path)
    println(sameModel.weights)
    sc.stop
  }
}
