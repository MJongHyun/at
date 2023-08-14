/*
날짜 - 요식업종 - 분기 - 시군구코드 별 없는 값 0으로 만들어주기

*/
package common.set_result_format

import common.jdbc.JdbcGetCollectData
import org.apache.spark.sql.DataFrame

class SetCateringDateQuarterSggFormat(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    val jdbcGetCollectDataObj = new JdbcGetCollectData(spark)
    val sggData = jdbcGetCollectDataObj.getAreaCode
    val cateringData = jdbcGetCollectDataObj.getCateringCategoryData

    val getDateQuarterFormatObj = new GetDateQuarterFormat(spark)
    val dateQuarterData = getDateQuarterFormatObj.getDateQuarterFormat(srvcStrtDt, srvcEndDt)

    (sggData, cateringData, dateQuarterData)
  }

  def getAllCateringDateQuarterSggDf(sggData: DataFrame,
                                     cateringData: DataFrame,
                                     dateQuarterData: DataFrame) = {
    val allCateringDateQuarterSgg = sggData.crossJoin(cateringData).crossJoin(dateQuarterData)

    allCateringDateQuarterSgg
  }

  def getCateringDateQuarterSggFormat(allCateringDateQuarterSgg: DataFrame, targetData: DataFrame) = {
    val getFormatDataPre = allCateringDateQuarterSgg.
      join(targetData,
        Seq("SGG_CODE", "mainCategory", "opnSvcNm", "uptaeNm", "DATE"),
        "leftouter")

    val getFormatData = getFormatDataPre.na.fill(0)

    getFormatData
  }

  def setCateringDateQuarterSggFormat(srvcStrtDt: String, srvcEndDt: String, targetData: DataFrame) = {
    val (sggData, cateringData, dateQuarterData) = getSourceData(srvcStrtDt, srvcEndDt)
    val allCateringDateQuarterSgg = getAllCateringDateQuarterSggDf(sggData, cateringData, dateQuarterData)
    val getFormatData = getCateringDateQuarterSggFormat(allCateringDateQuarterSgg, targetData)

    getFormatData
  }
}