import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
object NetWorkCount {
  def main(args: Array[String]): Unit = {
    //设置日志级别
    LoggerLevel.setStreamingLogLevels()
    //创建SparkConf并设置为本地模式运行
    //注意local[2]代表开两个线程
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWorkCount")
    //设置DStream批次时间间隔为2秒
    val ssc = new StreamingContext(conf, Seconds(2))
    //通过网络读取数据
    val lines = ssc.socketTextStream("localhost", 9999)
    //将读到的数据用空格切成单词
    val words = lines.flatMap(_.split(" "))
    //将单词和1组成一个pair
    val pairs = words.map(word => (word, 1))
    //按单词进行分组求相同单词出现的次数
    val wordCounts = pairs.reduceByKey(_ + _)
    //打印结果到控制台
    wordCounts.print()
    //开始计算
    ssc.start()
    //等待停止
    ssc.awaitTermination()
  }
}