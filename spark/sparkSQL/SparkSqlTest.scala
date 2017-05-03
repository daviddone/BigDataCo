
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{ Level, Logger }

object SparkSqlTest {
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class table_test(chnl_code:String,id_num:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //RDD隐式转换成DataFrame
    import sqlContext.implicits._
    //读取本地文件
    val table_test_DF = sc.textFile("D:\\david_work\\scala_work\\scala_mro_parser\\src\\test_table.txt")
      .map(_.split("\\t"))
      .map(d => table_test(d(0), d(1))).toDF()
    //分渠道进件数量统计并按进件数量降序排列
    table_test_DF.registerTempTable("table_test")
//    sqlContext.sql("select * from table_test").toDF().show()
    sqlContext.sql("" +
      "select chnl_code,count(*) as intpc_sum " +
      "from table_test " +
      "group by chnl_code").toDF().sort($"intpc_sum".desc).show()
  }

}

