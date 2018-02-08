package quickbook

import org.apache.spark._

//spark的累加器：类似于hadoop中的计数器 counter,spark2以后使用longAccucumulatot替代 accumulator
object Accumulator {
  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels();
    val conf = new SparkConf().setAppName("Accumulator")setMaster("local")
    val sc = new SparkContext(conf)
    val myRdd = sc.parallelize(Array(1,2,3,4,5,6,7))
    val evenNumbersCount = sc.longAccumulator("evenNumbersCount")//偶数
    var unevenNumbersCount = sc.accumulator(0, "Uneven numbers")
    myRdd.foreach(element =>
    {if (element % 2 == 0) evenNumbersCount.add(1L)
    else unevenNumbersCount +=1
    })
    println(s" Even numbers ${ evenNumbersCount.value }")
    println(s" Uneven numbers ${ unevenNumbersCount.value }")
  }
}