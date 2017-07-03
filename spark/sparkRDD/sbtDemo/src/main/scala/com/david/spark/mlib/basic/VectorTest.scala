package com.david.spark.mlib.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors

//密集向量 和 稀疏向量
object VectorTest {
  def main(args: Array[String]) {
    val vd = Vectors.dense(2, 0, 6) //密集向量
    println(vd(1))
    println(vd)

    //数据个数，序号，value 稀疏向量
    val vs = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 5, 2, 7))
    println(vs(2))
    println(vs)

    val vs2 = Vectors.sparse(4, Array(0, 2, 1, 3), Array(9, 5, 2, 7))
    println(vs2(2))
    println(vs2)


  }
}
