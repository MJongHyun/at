/*
Get CateringDensity Score

*/
package make_score.catering_density_score

import common.jdbc.{JdbcGetCollectData, JdbcGetMartData}
import org.apache.spark.sql.DataFrame

class GetCateringDensityScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    //  시군구 면적 데이터 (이미 시도 시군구 기준이라 시도 시군구 기준 코드 따로 안불러와도 됨)
    val getCollectDataObj = new JdbcGetCollectData(spark)
    val sggArea = getCollectDataObj.getSggArea
    //  지방인허가 데이터 요식업종별 시군구별 업체수 데이터
    val getMartDataObj = new JdbcGetMartData(spark)
    val cateringSggComCntData = getMartDataObj.getCateringSggComCntData(srvcStrtDt, srvcEndDt)

    (sggArea, cateringSggComCntData)
  }

  //  면적과 지방인허가 데이터 요식업종별 시군구별 업체수 데이터 시군구를 키로 조인
  def getCateringAddAreaData(sggArea: DataFrame, cateringMartData: DataFrame) = {
    val cateringAddAreaDataPre = sggArea.
      join(cateringMartData, Seq("SGG_CODE"))
    val cateringAddAreaData = cateringAddAreaDataPre.na.fill(0, Seq("comCount"))

    cateringAddAreaData
  }

  //  요식업종별 시군구별 업체수를 면적으로 나누기
  def getCateringComCntByArea(cateringAddAreaData: DataFrame) = {
    val cateringComCntByAreaPre = cateringAddAreaData.
      withColumn("comCntByArea", 'comCount.cast("int") / 'AREA.cast("int"))
    val cateringComCntByArea = cateringComCntByAreaPre.drop("comCount", "AREA")

    cateringComCntByArea
  }

  //  요식업종 밀집도 지수 구하기
  def getCateringDensityScore(srvcStrtDt: String, srvcEndDt: String) = {
    //  시군구 면적 데이터, 지방인허가 요식업종 요식업종별 시군구별 업체수 데이터 가져오기
    val (sggArea, cateringMartData) = getSourceData(srvcStrtDt, srvcEndDt)
    //  면적데이터, 업체수 데이터 조인
    val cateringAddAreaData = getCateringAddAreaData(sggArea, cateringMartData)
    //  면적대비 업체수
    val cateringComCntByArea = getCateringComCntByArea(cateringAddAreaData)

    cateringComCntByArea
  }
}