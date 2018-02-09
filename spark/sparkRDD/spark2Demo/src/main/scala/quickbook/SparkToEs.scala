package quickbook

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
//需要导入elasticsearch-hadoop-5.0.0.jar
import org.elasticsearch.spark._


object SparkToEs {

  def main(args: Array[String]): Unit = {
    StreamingExamples.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkToEs")
    conf.set("es.index.auto.create", "true")
//    conf.set("es.nodes", "107.191.61.156")//ip
    conf.set("es.nodes", "192.168.6.231")//ip
    conf.set("es.port", "9200")//port
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(6))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    pairs.print()
    val result = pairs.reduceByKey((a, b) => a + b)

    result.foreachRDD(rdd => {
      rdd.map(line => {
        val time = (System.currentTimeMillis() / 60000) * 60
        Map("hostIp" -> "hostIp",
          "word" -> line._1,
          "count" -> line._2.toLong, //累积数量
          "time" -> time)
      }).saveToEs("spark/docs")
    })
    //sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")spark/docs下的Elasticsearch索引内容（数字和机场）

    ssc.start()
    ssc.awaitTermination()
  }
}