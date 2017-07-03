package com.david.spark.mlib.basic

import org.apache.spark.mllib.linalg.Matrices

object MatrixTest {
  def main(args: Array[String]) {
    val mx = Matrices.dense(2, 3, Array(1, 2, 3, 4, 5, 6)) //创建一个分布式矩阵
    println(mx)

    val arr = (1 to 6).toArray.map(_.toDouble)
    val mx2 = Matrices.dense(2, 3, arr)
    println(mx2)

    val arr3 = (1 to 20).toArray.map(_.toDouble)
    val mx3 = Matrices.dense(4, 5, arr3)
    println(mx3.apply(0,0))
    println(mx3.apply(1,1))
    println(mx3.apply(2,2))
    println(mx3.numRows)
    println(mx3.numCols)
  }
}