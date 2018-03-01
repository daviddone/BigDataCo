package sql

import org.apache.spark.sql.SaveMode
// $example on:init_session$
import org.apache.spark.sql.SparkSession

case class Record2(key: Int, value: String)

object RDDRelation2x {
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder
      .appName("Spark Examples")
//      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //隐式转换
    import spark.implicits._
//    spark.sparkContext.textFile("examples/src/main/resources/people.txt")
//    val df =spark.sparkContext.parallelize((1 to 100)).map(i=>Record2(i,s"value_${i}")).toDF()
    val df = spark.createDataFrame((1 to 100).map(i => Record2(i, s"val_$i")))
    // 包含case实体类的所有RDD都可以注册为一张表（由scala反射自动测量）
    df.createOrReplaceTempView("Record2s")

    // 使用sql查询
    println("Result of SELECT *:")
    spark.sql("SELECT * FROM Record2s").collect().foreach(println)

    // 使用sql的聚类函数
    val count = spark.sql("SELECT COUNT(*) FROM Record2s").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // sql返回的是rdd可以使用rdd函数操作
    val rddFromSql = spark.sql("SELECT key, value FROM Record2s WHERE key < 10 order by key desc")

    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // 可以用类似Scala DSL查询
    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)

    // 使用overwrite mode模式将rdd村委parquet文件
    df.write.mode(SaveMode.Overwrite).parquet("pair.parquet")

    // 读取parquet文件
    val parquetFile = spark.read.parquet("pair.parquet")

    // 像rdd一样查询
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    // parquet文件可直接注册为表
    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile").collect().foreach(println)

    spark.stop()
  }
}

