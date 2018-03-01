
// scalastyle:off println
package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import quickbook.StreamingExamples

//采用spark1.6的方式SQLContext
case class Record(key: Int, value: String)

object RDDRelation1x {
  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("RDDRelation")
    .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // 导入SQL上下文可以访问所有SQL函数和隐式转换。
    import sqlContext.implicits._
    val records:RDD[Record] = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
//    records.foreach(println)
    val df = records.toDF()
    // 包含case实体类的所有RDD都可以注册为一张表（由scala反射自动测量）
    df.registerTempTable("records")

    // 表注册之后就可以查询.
    println("Result of SELECT *:")
    sqlContext.sql("SELECT * FROM records").collect().foreach(println)

    // 支持聚合查询
    val count = sqlContext.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    //SQL的查询结果是RDD，支持所有的RDD函数 结果通过row行返回
    val rddFromSql = sqlContext.sql("SELECT key, value FROM records WHERE key < 10 order by key desc")

    println("Result of RDD.map:")
//    rddFromSql.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)
    rddFromSql.map(row => "this is "+row(0)+"  "+row(1)).collect().foreach(println)

    // 类scala方式获取数据
    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)

    // 将rdd数据写成parquet格式输出 parquet的优势：https://www.cnblogs.com/piaolingzxh/p/5469964.html
    df.write.parquet("pair.parquet")

    //读取parquet文件
    val parquetFile = sqlContext.read.parquet("pair.parquet")

    // 可以用RDD的操作方式直接使用
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    // parquet文件可以直接用来注册表
    parquetFile.registerTempTable("parquetFile")
    sqlContext.sql("SELECT * FROM parquetFile").collect().foreach(println)

    sc.stop()
  }
}
// scalastyle:on println
