import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// spark 按照时间窗口关联最近数据
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
    val filterLines = mdtLines.filter(eachLine => {
      var fields = eachLine.split(",")
      val timestamp = fields(0)
      timestamp.contains("-")
    })
    filterLines.foreach(x => {
      println("mdt每一行数据:"+x)
    })
    val words = filterLines.flatMap(eachLine => eachLine.split(","))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(x => {
      println("mdt数据："+x)
    })

  }
}
