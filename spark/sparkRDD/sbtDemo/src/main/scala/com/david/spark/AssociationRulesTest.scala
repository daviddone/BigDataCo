package com.david.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.{SparkConf, SparkContext}

// 关联规则
object AssociationRulesTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("AssociationRulesTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val freqItemsets = sc.parallelize(Seq(
      new FreqItemset(Array("a"), 15L),
      new FreqItemset(Array("b"), 35L),
      new FreqItemset(Array("a", "b"), 12L)
    ))   //FreqItemset 频繁项集集合 12L为long类型的频率

    val ar = new AssociationRules()
      .setMinConfidence(0.8) //设置最小置信度  x 发生时，y发生的概率
    val results = ar.run(freqItemsets)

    results.collect().foreach { rule =>
      println("[" + rule.antecedent.mkString(",")
        + "->"
        + rule.consequent.mkString(",") + "]," + rule.confidence)
    } //antecedent先决条件x -> consequent结果y

  }
}
