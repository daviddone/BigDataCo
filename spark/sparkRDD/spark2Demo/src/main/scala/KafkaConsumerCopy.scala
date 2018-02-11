import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{HashPartitioner, SparkConf}

object KafkaConsumerCopy {
//  private val checkpointDir = "hdfs://spark001:9000/checkpoint/Kafka2Hdfs"
  val outDir = "/user/boco/david/Kafka2Hdfs"
  private val checkpointDir = "/user/boco/david/checkpoint/Kafka2Hdfs"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"
  def main(args: Array[String]) {
    if (args.length < 2){
      System.exit(1)
  }
    val Array(zkServers, processingInterval) = args
    val conf = new SparkConf().setAppName("user-behavior-topic")
    val ssc = new StreamingContext(conf, Seconds(processingInterval.toInt))
    //使用updateStateByKey要求启用检查点
    ssc.checkpoint(checkpointDir)
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      zkServers,
      msgConsumerGroup,
      Map("user-behavior-topic" -> 3))

    val msgLinesRDD = kafkaStream.map(_._2) //生产者传递topic,line
    //println("Coming data in this interval…")
    msgLinesRDD.print()
    // e.g page37|5|1.5119122|-1
    //计算每个界面的热度
    val popularityData = msgLinesRDD.map { msgLine => {
      val dataArr: Array[String] = msgLine.split("\\|")
      val pageID = dataArr(0)
      val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
      (pageID, popValue) //(page37,45)
    }
    }
    //之前的热度值与现在进行求和
    val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
      iterator.flatMap(t => {
        val newValue: Double = t._2.sum
        val stateValue: Double = t._3.getOrElse(0);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))

    }

    val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))

    val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    print(stateDstream)
    //设置检查点间隔以避免可能太频繁的数据检查点
    //可能会大大降低运营吞吐量
    stateDstream.checkpoint(Duration(8 * processingInterval.toInt * 1000))
    //经过计算，对结果进行排序，只显示前10个热门页面sortByKey(true)升序
    stateDstream.foreachRDD { rdd => {
      val sortedData = rdd.map { case (k, v) => (v, k) }.sortByKey(false)
      val topKData = sortedData.take(10).map  { case (v, k) => (k, v) }
      topKData.foreach(x => {
        println(x)   //最后结果为page37,47
      })
      val finalRDD = ssc.sparkContext.parallelize(topKData);
      finalRDD.saveAsTextFile(outDir) //只能rdd才能调用saveAsTextFile
//      finalRDD.partitions(1).saveAsTextFile("outDir")  //只输出一个文件
    }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}