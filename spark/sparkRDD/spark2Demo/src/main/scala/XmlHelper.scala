import scala.xml._
import javax.xml.parsers.SAXParserFactory

object XmlHelper {
  def main(args: Array[String]): Unit = {
    val filePath = "data/TD-LTE_MRO_ZTE_OMC1_975750_20170222180000.xml"
//      val filePath = "data/FDD-LTE_MRO_NSN_OMC_659001_20160817111500_sample.xml"
    xml2csv(filePath)
  }
  //将原始数据xml转换为csv格式
  def xml2csv(filePath:String): Unit ={
    val rawData = scala.io.Source.fromFile(filePath).mkString
    //    println(rawData)
    val mroXml = loadOfflineXML(rawData)
    val enbId = (mroXml \\ "eNB" \ "@id").text   // 类似xpath语法 一个属性
    val smrs = (mroXml \\ "smr").text.split("\\s").toList // \s空格 转义字符
    val smrVs = (mroXml \\ "eNB" \ "measurement"\ "object" ).toList
    for( item <- smrVs ){
      val eachData = loadOfflineXML(item.mkString)
      //      print(eachData)
      val objectId = (eachData \\ "object").map(_\"@id") //多个属性
      val mmeUeS1apId = (eachData \\ "object").map(_\"@MmeUeS1apId")
      val mmeGroupId = (eachData \\ "object").map(_\"@MmeGroupId")
      val mmeCode = (eachData \\ "object").map(_\"@MmeCode")
      val timeStamp = (eachData \\ "object").map(_\"@TimeStamp")
      val objectVs = (eachData \\ "object"\"v").toList
      val objectAttrs = objectId.mkString.concat(",")
        .concat(mmeUeS1apId.mkString).concat(",")
        .concat(mmeGroupId.mkString).concat(",")
        .concat(mmeCode.mkString).concat(",")
        .concat(timeStamp.mkString)
      //      println("item:"+objectAttrs)
      for(eachV <- objectVs){
        var newEachV = eachV.mkString.replace("<v>","").replace("</v>","").replaceAll("\\s",",")
        val csvLine =enbId.concat(",")
          .concat(objectAttrs).concat(",")
          .concat(newEachV)
        println(csvLine)
      }
    }
  }
  def offlineParser: SAXParser = {
    val f = SAXParserFactory.newInstance()
    f.setNamespaceAware(false)
    f.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
    f.newSAXParser()
  }

  def loadOfflineXML(source: String): Elem = {
    XML.loadXML(scala.xml.Source.fromString(source), offlineParser)
  }

}