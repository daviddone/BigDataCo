

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

object LoggerLevel extends Logging {

  def setStreamingLogLevels() {
    Logger.getLogger("org").setLevel(Level.ERROR)
//    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
//    if (!log4jInitialized) {
//      Logger.getRootLogger.setLevel(Level.WARN)
//    }
  }
}