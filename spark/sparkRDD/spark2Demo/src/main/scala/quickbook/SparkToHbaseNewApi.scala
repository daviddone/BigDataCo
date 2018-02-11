package com.smart.hbase

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import quickbook.StreamingExamples

object SparkToHbaseNewApi {

  def main(args: Array[String]): Unit = {

//    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("SparkToHbaseNewApi").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val tablename = "david"
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.zookeeper.quorum","hadoop231,hadoop232,hadoop234")
    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    hConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")// 避免：java.lang.IllegalArgumentException: Can not create a Path from a null string

    val jobConf = new JobConf(hConf, this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    //设置job的输出格式
    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])


    val indataRDD = sc.makeRDD(Array("4,jack,15","5,Lily1,16","6,mike,16"))

    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    }}
    //使用新API保存到HBase表
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()
  }

}
