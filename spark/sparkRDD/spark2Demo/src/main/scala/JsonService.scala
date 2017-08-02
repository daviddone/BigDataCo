import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object JsonService {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val strAppName = "Json2SQL"
    //方便调试，如果运行在集群上，需要将master设置为yarn-cluster
    var strMaster = "local[*]"
    if (args.length == 1) {
      strMaster = args(0)
    }
    val spark = SparkSession
      .builder()
      .master(strMaster)
      .appName(strAppName)
      .getOrCreate()
    val df_ip = spark.read.json("data/asset_ip.json") //多条json数据
    df_ip.createOrReplaceTempView("asset_ip")
    val df_ip_update = spark.read.json("data/asset_ip_update.json")
    df_ip_update.createOrReplaceTempView("asset_ip_update")
    import spark.sql
    println(sql("SELECT * FROM asset_ip r RIGHT JOIN asset_ip_update l ON r.ip = l.ip").show())
    //sc.stop()
    spark.stop()
  }
}
