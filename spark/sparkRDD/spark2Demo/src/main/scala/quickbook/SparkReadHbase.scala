package quickbook

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io._

object SparkReadHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkReadHbase").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tablename = "david"
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.zookeeper.quorum","hadoop231,hadoop232,hadoop234")
    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    hConf.set(TableInputFormat.INPUT_TABLE, tablename)

    // 如果表不存在则创建表
    val connection = ConnectionFactory.createConnection(hConf)
//    val admin = new HBaseAdmin(hConf)
    val admin = connection.getAdmin
    if (!admin.isTableAvailable(TableName.valueOf(tablename))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println(count)
    hBaseRDD.foreach{case (_,result) =>{
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
      val age = Bytes.toInt(result.getValue("cf".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
    }}

    sc.stop()
    admin.close()
  }
}