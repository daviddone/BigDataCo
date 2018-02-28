package stream

import org.apache.spark._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import quickbook.StreamingExamples

import scala.io.Source

object AvroSparkConsumer {
  def main(args: Array[String]): Unit = {
//    val args=Array("vm10.60.0.8.com.cn:2181,vm10.60.0.7.com.cn:2181,vm10.60.0.11.com.cn:2181","2")
    StreamingExamples.setStreamingLogLevels()
    val args=Array("107.191.61.156:2181","2")
    if (args.length < 2) {
      System.err.println("Usage: <topic> <numThreads>")
      System.exit(1)
    }
    val Array(zks, numThreads) = args
    println(zks)
    println(numThreads)
    val sparkConf = new SparkConf().setAppName("WindowClickCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("/user/boco/david/checkPointAvro")
//    val brokerlist = "vm10.60.0.8.com.cn:9092,vm10.60.0.7.com.cn:9092"
    val brokers = "107.191.61.156:9092"
    //创建能够可以解码字节数组
    val topics="test007"
    val topicsSet = topics.split(",").toSet
//    val topicsSet = Set("test007")
//    val topicMap = Map("test007"->2)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)
//    val messages = KafkaUtils.createStream[String, Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, Map("test007"->2), StorageLevel.MEMORY_ONLY_SER)
    val requestLines =  messages.map(_._2)
    // 将二进制数据转换成对象 并打印出来
    val newRDD = requestLines.foreachRDD(rdd => rdd.map({
      bytes=>
        val data: GenericRecord = AvroUtil.deserializeMessage(bytes)
        println("data:"+data.get("sites"))
      }
    ))
    ssc.start()
    ssc.awaitTermination()
  }
}
object AvroUtil {
  // 将二进制数据转换为avro对象
  // https://cwiki.apache.org/confluence/display/AVRO/FAQtil {
//  val reader = new SpecificDatumReader[ClickEvent](ClickEvent.getClassSchema)
//  def deserializeMessage(bytes: Array[Byte]): ClickEvent = {
//    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
//    reader.read(null, decoder)
//  }
//  val schemaString: String = Source.fromURL(getClass.getResource("data/avro/user.avsc")).mkString
  val schemaString: String = Source.fromFile("data/avro/user.avsc").mkString
  val schema: Schema = new Schema.Parser().parse(schemaString)
  def deserializeMessage(msg: Array[Byte]): GenericRecord = {
    val reader: SpecificDatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get.binaryDecoder(msg, null)
    val data: GenericRecord = reader.read(null, decoder)
    data
  }
}
