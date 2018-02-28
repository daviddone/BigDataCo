package stream

import java.io.ByteArrayOutputStream
import java.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import stream.AvroUtil.getClass

import _root_.scala.util.Random
import scala.io.Source


object AvroKafkaProducer  {
  def main(args: Array[String]): Unit = {
//    val args=Array("vm10.60.0.8.com.cn:2181,vm10.60.0.7.com.cn:2181,vm10.60.0.11.com.cn:2181","avro-topic")
//    val args=Array("107.191.61.156:2181","test007")
    if (args.length < 2) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic>")
      System.exit(1)
    }

    val Array(brokers, topic) = args

    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)

    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.ByteArraySerializer") // Kafka avro message stream comes in as a byte array
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, Array[Byte]](props)

    // Send some kafka click events at 1 second intervals
    while(true) {
      val serializedBytes = serialize() // Avro schema serialization as a byte array
      // send message
      val queueMessage = new ProducerRecord[String, Array[Byte]](topic, serializedBytes)
      println("send message!")
      producer.send(queueMessage)
      Thread.sleep(1000)
    }

    // Serialize a click event using Avro into a byte array to send through Kafka
    // https://cwiki.apache.org/confluence/display/AVRO/FAQ
    def serialize(): Array[Byte] = {
//      val out = new ByteArrayOutputStream()
//      val encoder = EncoderFactory.get.binaryEncoder(out, null)
//      val writer = new SpecificDatumWriter[ClickEvent](ClickEvent.getClassSchema)
//      writer.write(clickEvent, encoder)
//      encoder.flush
//      out.close
//      out.toByteArray

      //读取schema模式文件
//      val schema: Schema = new Parser().parse(Scource.fromURL(getClass.getResoure("data/avro/user.avsc")).mkString)
      val schemaString: String = Source.fromFile("data/avro/user.avsc").mkString
      val schema: Schema = new Schema.Parser().parse(schemaString)

      val record: GenericRecord = new GenericData.Record(schema)
      record.put("batch_id", java.util.UUID.randomUUID.toString)
      record.put("sites", "zhangsan")
      record.put("timestamp", "2017-01-01")
      // 序列化二进制
      val writer = new SpecificDatumWriter[GenericRecord](schema)
      val out = new ByteArrayOutputStream()
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(record, encoder)
      encoder.flush()
      out.close()
      val serializedBytes: Array[Byte] = out.toByteArray
      serializedBytes
    }
  }

}
