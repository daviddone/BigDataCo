import index.{IndexMdtSourceEnum, IndexUemrBaseEnum, TimeUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 关联mdt和uemr海量数据，通过pid和timeDiff筛选唯一纪录
  */
object SparkJoin {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var strMaster = "local[*]"
    var strAppName = "sparkjoin"
    if (args.length == 1) {
      strMaster = args(0)
    }
    val conf = new SparkConf().setAppName(strAppName).setMaster(strMaster)
    val sc = new SparkContext(conf)
    val uemrLines = sc.textFile("data/join/uemr_data.txt")
    val mdtLines = sc.textFile("data/join/mdt_data.csv")
    println(uemrLines.count())
    uemrLines.foreach(println)
    mdtLines.foreach(println)//mdt
    //过滤数据
    val mdtFilterLines = mdtLines.filter(eachLine => {
      var fields = eachLine.split(",")
      val timestamp = fields(0)
      timestamp.contains("-")
    })
    mdtFilterLines.foreach(x => {
      println("mdt每一行数据:"+x)
    })
    val mdtMap = mdtFilterLines.map(x => (x.split(",")(6),x))//MMEUES1APID(6) as key
    val uemrMap = uemrLines.map(x => {
      val uemrLength = x.split("\\|").length //注意竖线时候需要转义
      println("uemr字段个数："+uemrLength)
      (x.split("\\|")(uemrLength-1),x)
    })
    val joinRDD = mdtMap.join(uemrMap)
    val joinRDDeTL = joinRDD.map(x => {
      (x._2._1.replace(",","|"),x._2._2)
    })
    joinRDDeTL.foreach(x => {
      println("合并之后的数据："+x._1+"##"+x._2)
    })
    println("第一次join的计数："+joinRDD.values.count())
    val diffTimeMap = joinRDD.values.map(x => {
      val mdtOthers = x._1.substring(x._1.indexOf(",")+1,x._1.length)//1,2,3
      println("mdtOthers"+mdtOthers)
      var beginTime = x._1.substring(0,x._1.indexOf(","))
      val newBeginTime = beginTime.replace("T"," ")
      val mdtNewOthers = mdtOthers.replace(",","|")
      println("newBeginTime:"+newBeginTime)
      println("mdtNewOthers:"+mdtNewOthers)
      val mdtNewLine = newBeginTime + "|" + mdtNewOthers
      println("mdt新数据:"+mdtNewLine)
      val uemrFileds = x._2.split("\\|")
      val mdtTimeLong = TimeUtil.getTimeToTick(newBeginTime).toLong
      val uemrTimeLong = TimeUtil.getTimeToTick(uemrFileds(IndexUemrBaseEnum.MRTIME_STAMP.getIndex)).toLong
      print("mdtTimeLong:" + mdtTimeLong )
      print("uemrTimeLong:" + uemrTimeLong )
      val diffTime = Math.abs(mdtTimeLong-uemrTimeLong) //ms
      val newLine = mdtNewLine + "##" +x._2
      println("合并之后的数据2："+mdtNewLine + "##" +x._2)
      val mdtNewFields = mdtNewLine.split("\\|")
      val key = mdtNewFields(IndexMdtSourceEnum.MMEUES1APID.getIndex()) + "##"+mdtNewFields(IndexMdtSourceEnum.TIMESTAMP.getIndex())
      (key,(diffTime,newLine))// (pid##time,(difftime,joinedline))
    })
    val finalMap1 = diffTimeMap.groupByKey().mapValues(iter => iter.toList.sortWith(_._1 > _._1).take(1))
    //item （key,(timediff,line)）
    val minTime = ConstantsUtil.diffTime.toInt*1000
    val finalMap2 = finalMap1.filter(item=>{
//      print("list:"+item._2)
      item._2(0)._1.toInt < minTime
    })

    finalMap1.foreach(x => {
      println("最终数据diffTime："+x._2(0)._1)
      println("最终数据："+x._2(0)._2)
    })
    finalMap2.foreach(x => {
      println("过滤之后的diffTime："+x._2(0)._1)
      println("过滤之后的数据："+x._2(0)._2)
    })
//    finalMap2.repartition(1).saveAsTextFile("data/output/1.txt")
  }
}
