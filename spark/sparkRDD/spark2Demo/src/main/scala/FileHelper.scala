import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object FileHelper {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileUtil").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile("data/TD-LTE_MRO_ZTE_OMC1_975750_20170222180000.xml")
    textRDD.foreach(println)//打印出每行数据
    println("数据行数:"+textRDD.count())//统计文件行数
    Source.fromFile("data/TD-LTE_MRO_ZTE_OMC1_975750_20170222180000.xml" ).foreach{
      print
    }
    val wholeText = Source.fromFile("data/TD-LTE_MRO_ZTE_OMC1_975750_20170222180000.xml" ).mkString
    val wholeLines = Source.fromFile("data/TD-LTE_MRO_ZTE_OMC1_975750_20170222180000.xml").getLines()
  }

  def writeData(line:String,fileName:String): Unit ={
    val writer = new PrintWriter(new File(fileName))
    writer.write(line)
    writer.close()
  }
}
