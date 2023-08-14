package make_score.catering_activity_score

import common.jdbc.{JdbcGetMartData, JdbcGetPublicData}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class GetCateringActivityScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    // 지방인허가 데이터 요식업종별 시군구별 업체수 데이터 추출
    val getMartDataObj = new JdbcGetMartData(spark)
    val cateringSggComCntData = getMartDataObj.getCateringSggComCntData(srvcStrtDt, srvcEndDt)
    //월별 시군구별 거주 인구 데이터 추출
    val JdbcGetPublicDataObj = new JdbcGetPublicData(spark)
    val sggMonthSeriesResidentPopulation = JdbcGetPublicDataObj.
      getSggMonthSeriesResidentPopulation(srvcStrtDt, srvcEndDt)
    //  국민연금 데이터기반 월별 업체 근로자수 데이터 추출
    val getMartDataObj2 = new JdbcGetMartData(spark)
    val monthSeriesPensionAmtMemberData = getMartDataObj2.getPensionPopMart(srvcStrtDt, srvcEndDt)

    (cateringSggComCntData, sggMonthSeriesResidentPopulation, monthSeriesPensionAmtMemberData)
  }

  // 활성도지수를 추출하기 위한 마트데이터 추출
  def getCateringActivityMartData(cateringSggComCntData:DataFrame,
                                  sggMonthSeriesResidentPopulation:DataFrame,
                                  monthSeriesPensionAmtMemberData:DataFrame)  = {
    // 월별 시군구 요식업 업체 수 추출
    val cateringSggCntDataPre = cateringSggComCntData.
      groupBy("DATE", "SGG_CODE").
      agg(sum("comCount") as "sggComCnt")
    val cateringSggCntData = cateringSggCntDataPre
    // 활성도지수에 필요한 월별 시군구 근로자 인구 수, 거주자 인구 수, 요식업 업체 수 마트데이터 추출
    val cateringActivityMartPre1 = cateringSggCntData.
      join(sggMonthSeriesResidentPopulation, Seq("DATE", "SGG_CODE"), "outer")
    val cateringActivityMartPre2 = cateringActivityMartPre1.
      join(monthSeriesPensionAmtMemberData, Seq("DATE", "SGG_CODE"), "outer")
    val cateringActivityMartPre = cateringActivityMartPre2.
      na.fill(0).
      select("DATE", "SGG_CODE", "sggComCnt", "TTL_POP", "workingPop")

    cateringActivityMartPre
  }

  // 월별 요식업 업체 수 대비 인구 수 결과를 구한 후, 편차를 통해 활성도 지수 값 추출
  def getDeviationData(cateringActivityMartData:DataFrame) = {
    // 월별 요식업 업체 수 + 1 대비 인구 수 값 추출
    val activityValueData = cateringActivityMartData.
      withColumn("actScorePre", ('TTL_POP + 'workingPop)/('sggComCnt + 1))
    // 월별 요식업 업체 수 대비 인구 수 평균 값 추출
    val meanData = activityValueData.
      groupBy("BASE_YM").
      agg(avg('actScorePre) as "mean_actScorePre")
    // 편차를 통해 활성도 지수 값 추출
    val deviationDataPre1 = activityValueData.
      join(meanData, Seq("BASE_YM"))
    val deviationDataPre2 = deviationDataPre1.
      withColumn("RBIZ_TOID_ACVT_IDEX_RAW", 'actScorePre - 'mean_actScorePre)
    val deviationDataPre3 = deviationDataPre2.
      withColumn("RBIZ_TOID_ACVT_IDEX", round('RBIZ_TOID_ACVT_IDEX_RAW, 10))
    val deviationData = deviationDataPre3.
      select("BASE_YM", "CTNP_CODE", "SGGU_CODE", "CTNP_NM", "SGGU_NM", "RBIZ_TOID_ACVT_IDEX")

    deviationData
  }

  //  요식업종 활성도지수 구하기
  def getCateringActivityScore(srvcStrtDt:String, srvcEndDt:String) = {
    // 시군구 코드 데이터, 지방인허가 시군구/업종 업체수 데이터, 시군구 거주자 인구 데이터, 시군구 근로인구 데이터 저장
    val (cateringSggComCntData, sggMonthSeriesResidentPopulation, monthSeriesPensionAmtMemberData) =
      getSourceData(srvcStrtDt, srvcEndDt)
    // 활성도지수를 추출하기 위한 마트데이터 추출 (연월 시군구에 따르는 요식업 업종 수, 거주자 인구, 근로인구)
    val cateringActivityMartData1 =
      getCateringActivityMartData(cateringSggComCntData, sggMonthSeriesResidentPopulation, monthSeriesPensionAmtMemberData)
    // 마트 데이터부터 형식 맞추기 (편차구해야해서 타겟 시군구 이런거 먼저 맞춰야함)
    val setCateringActivityResultFormatObj = new SetCateringActivityResultFormat(spark)
    val cateringActivityMartData = setCateringActivityResultFormatObj.
      setCateringActivityResultFormat(srvcStrtDt, srvcEndDt, cateringActivityMartData1)
    // 월별 시군구 요식업 업종 수 대비 인구 수 결과를 구한 후, 편차를 통해 활성도 지수 값 추출
    val cateringActivityScorePre = getDeviationData(cateringActivityMartData)

    cateringActivityScorePre
  }
}