/*
요식업 밀집도 지수: 요식업종, 시군구, 날짜 누락된 데이터 없도록 0인 값 채워주기

*/
package make_score.catering_density_score

import common.set_result_format.SetCateringDateSggFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, regexp_replace, round}

class SetCateringDensityResultFormat(spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  //  기준 맞춰주기 전 필요한 컬럼만
  def getCateringDensityResultPre(cateringComCntByArea: DataFrame) = {
    val cateringDensityResultPre1 = cateringComCntByArea.
      withColumn("comCntByArea_round", round('comCntByArea, 10))
    val cateringDensityResultPre = cateringDensityResultPre1.
      select(
        'DATE.as("BASE_YM"),
        'SGG_CODE.as("SGGU_CODE"),
        'mainCategory.as("RBIZ_TOID_LGLS_NM"),
        'opnSvcNm.as("RBIZ_TOID_MDCL_NM"),
        'uptaeNm.as("RBIZ_TOID_SMCS_NM"),
        'comCntByArea_round.as("RBIZ_TOID_DST_IDEX")
      )

    cateringDensityResultPre
  }

  //  소분류명에 , 있는 값 / 로 변환
  def setCateringDensityNameFormat(cateringDensityScorePre: DataFrame) = {
    val cateringDensityScorePre1 = cateringDensityScorePre.withColumn("uptaeNmPre", 'RBIZ_TOID_SMCS_NM)
    val cateringDensityScorePre2 = cateringDensityScorePre1.drop("RBIZ_TOID_SMCS_NM")
    val cateringDensityScorePre3 = cateringDensityScorePre2.
      withColumn("RBIZ_TOID_SMCS_NM", regexp_replace('uptaeNmPre, lit(","), lit("/")))
    val cateringDensityScore = cateringDensityScorePre3.drop("uptaeNmPre")

    cateringDensityScore
  }

  def setCateringDensityResultFormat(srvcStrtDt: String, srvcEndDt: String, cateringComCntByArea: DataFrame) = {
    val cateringDensityResultPre = getCateringDensityResultPre(cateringComCntByArea)

    val setCateringDateSggFormatObj = new SetCateringDateSggFormat(spark)
    val cateringDensityScorePre = setCateringDateSggFormatObj.
      setCateringDateSggFormat(srvcStrtDt, srvcEndDt, cateringDensityResultPre)

    val cateringDensityScore = setCateringDensityNameFormat(cateringDensityScorePre)

    cateringDensityScore
  }
}