/*
요식업종 - 날짜 - 시군구코드 별 없는 값 0으로 만들어주기

*/
package common.set_result_format

import common.jdbc.JdbcGetCollectData

import org.apache.spark.sql.DataFrame

class SetCateringDateSggFormat(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    val jdbcGetCollectDataObj = new JdbcGetCollectData(spark)
    val sggData = jdbcGetCollectDataObj.getAreaCode
    val cateringData = jdbcGetCollectDataObj.getCateringCategoryData

    val getDateFormatObj = new GetDateFormat(spark)
    val dateData = getDateFormatObj.getDateFormat(srvcStrtDt, srvcEndDt)

    (sggData, cateringData, dateData)
  }

  def getAllCateringDateSggDf(sggData: DataFrame, cateringData: DataFrame, dateData: DataFrame) = {
    val allCateringDateSggPre = sggData.crossJoin(cateringData).crossJoin(dateData)
    val allCateringDateSgg = allCateringDateSggPre.
      select(
        'DATE.as("BASE_YM"),
        'SIDO_CODE.as("CTNP_CODE"),
        'SGG_CODE.as("SGGU_CODE"),
        'SIDO.as("CTNP_NM"),
        'SGG.as("SGGU_NM"),
        'mainCategory.as("RBIZ_TOID_LGLS_NM"),
        'opnSvcNm.as("RBIZ_TOID_MDCL_NM"),
        'uptaeNm.as("RBIZ_TOID_SMCS_NM")
      )

    allCateringDateSgg
  }

  def getCateringDateSggFormat(allCateringDateSgg: DataFrame, targetData: DataFrame) = {
    val getFormatDataPre = allCateringDateSgg.
      join(targetData, Seq("SGGU_CODE", "RBIZ_TOID_LGLS_NM", "RBIZ_TOID_MDCL_NM", "RBIZ_TOID_SMCS_NM", "BASE_YM"), "leftouter")

    val getFormatData = getFormatDataPre.na.fill(0)

    getFormatData
  }

  def setCateringDateSggFormat(srvcStrtDt: String, srvcEndDt: String, targetData: DataFrame) = {
    val (sggData, cateringData, dateData) = getSourceData(srvcStrtDt, srvcEndDt)
    val allCateringDateSgg = getAllCateringDateSggDf(sggData, cateringData, dateData)
    val getFormatData = getCateringDateSggFormat(allCateringDateSgg, targetData)

    getFormatData
  }
}