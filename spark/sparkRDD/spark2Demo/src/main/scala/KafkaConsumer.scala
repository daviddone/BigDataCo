import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaConsumer {
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
    val words = msgLinesRDD.flatMap(_.split("\\|"))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    // e.g page37|5|1.5119122|-1
    wordCounts.saveAsTextFiles(outDir,"hah")
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}