import org.apache.spark._
import org.apache.spark.streaming._

object MyFirstStreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9999) //windows nc -L -p 9999 -v
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print() //(zhangsan,3) (lisi,1)

    ssc.start()
    ssc.awaitTermination()

  }
}
