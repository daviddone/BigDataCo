package quickbook

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
//需要导入elasticsearch-spark_2.11.jar替换elasticsearch-hadoop-5.6.1.jar
import org.elasticsearch.spark._


object SparkReadEs {

  def main(args: Array[String]): Unit = {
//    StreamingExamples.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[1]").setAppName("SparkReadEs")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "192.168.6.231")//ip
    conf.set("es.port", "9200")//port
    val sc = new SparkContext(conf)
    val rdd = sc.esRDD("spark/docs")
//    sc.esRDD("radio/artists", "?q=me*")   匹配关键字的相关数据
    rdd.foreach(x=>print(x))//所有数据
    print(rdd.count())  //数据总计
  }
}