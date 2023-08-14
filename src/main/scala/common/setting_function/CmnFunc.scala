/*
공통 사용 기능 (spark, logger)

*/
package common.setting_function

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

class CmnFunc {
  // spark
  def getSpark() = {
    val spark: SparkSession = SparkSession.
      builder.
      appName("AT").
      getOrCreate()

    spark
  }

  // logger
  def getLogger() = {
    val logger = LogManager.getLogger("atLogger")
    logger
  }
}