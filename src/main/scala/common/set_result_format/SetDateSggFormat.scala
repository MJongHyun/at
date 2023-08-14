/*
날짜 - 시군구코드 별 없는 값 0으로 만들어주기

*/
package common.set_result_format

import common.jdbc.JdbcGetCollectData
import org.apache.spark.sql.DataFrame

class SetDateSggFormat(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    val jdbcGetCollectDataObj = new JdbcGetCollectData(spark)
    val sggData = jdbcGetCollectDataObj.getAreaCode

    val getDateFormatObj = new GetDateFormat(spark)
    val dateData = getDateFormatObj.getDateFormat(srvcStrtDt, srvcEndDt)

    (sggData, dateData)
  }

  def getAllDateSggDf(sggData: DataFrame, dateData: DataFrame) = {
    val allDateSggPre = sggData.crossJoin(dateData)
    val allDateSgg = allDateSggPre.
      select(
        'DATE.as("BASE_YM"),
        'SIDO_CODE.as("CTNP_CODE"),
        'SGG_CODE.as("SGGU_CODE"),
        'SIDO.as("CTNP_NM"),
        'SGG.as("SGGU_NM")
      )

    allDateSgg
  }

  def getDateSggFormat(allDateSgg: DataFrame, targetData: DataFrame) = {
    val getFormatDataPre = allDateSgg.
      join(targetData, Seq("SGGU_CODE", "BASE_YM"), "leftouter")

    val getFormatData = getFormatDataPre.na.fill(0)

    getFormatData
  }

  def setDateSggFormat(srvcStrtDt: String, srvcEndDt: String, targetData: DataFrame) = {
    val (sggData, dateData) = getSourceData(srvcStrtDt, srvcEndDt)
    val allDateSgg = getAllDateSggDf(sggData, dateData)
    val getFormatData = getDateSggFormat(allDateSgg, targetData)

    getFormatData
  }
}