/*
Jdbc Get PublicData

*/
package common.jdbc

import org.apache.spark.sql.functions.{coalesce, concat, lit, substring, typedLit}

class JdbcGetPublicData(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  val (url, connProp) = publicDataJdbcProp()

  //  월별 시군구별 거주 인구
  def getSggMonthSeriesResidentPopulation(srvcStrtDt: String, srvcEndDt: String) = {
    // 시군구 코드 예외 처리 남구 -> 미추홀구
    val sggCodeExceptMapCol = typedLit(Map("28170" -> "28177"))
    val allResidentPopulation = spark.read.jdbc(url, "CLCTDATA.MOIS_RR_HH", connProp)
    //  행정동 단위 데이터 필요 없어서 버리기 & 필요한 데이터 기간 안의 데이터만 추출
    val sggMonthSeriesResidentPopulationPre1 = allResidentPopulation.
      filter('HJD_NM.isNull).
      filter('DATE >= srvcStrtDt.toInt && 'DATE <= srvcEndDt.toInt)
    val sggMonthSeriesResidentPopulationPre2 = sggMonthSeriesResidentPopulationPre1.
      select(
        'DATE,
        substring('HJD_CD, 1, 5).as("SGG_CODE_PRE"),
        'TTL_POP
      )
    val sggMonthSeriesResidentPopulationPre3 = sggMonthSeriesResidentPopulationPre2.
      withColumn("SGG_CODE", coalesce(sggCodeExceptMapCol('SGG_CODE_PRE), 'SGG_CODE_PRE))

    val sggMonthSeriesResidentPopulation = sggMonthSeriesResidentPopulationPre3.drop("SGG_CODE_PRE")

    sggMonthSeriesResidentPopulation
  }

  //  국민연금 데이터로 월별 업체 국민연금 납부액, 근로자수 추출
  def getMonthSeriesPensionAmtMemberCnt(srvcStrtDt: String, srvcEndDt: String) = {
    // 시군구 코드 예외 처리
    // 남구 28170 -> 미추홀구 28177
    // 부천시 원미구 41195 -> 부천시 41190
    // 부천시 소사구 41197 -> 부천시 41190
    // 부천시 오정구 41199 -> 부천시 41190
    // 화성시 동부 출장소 41592 -> 화성시 41590
    val sggCodeExceptMapCol = typedLit(Map(
      "28170" -> "28177",
      "41195" -> "41190",
      "41197" -> "41190",
      "41199" -> "41190",
      "41592" -> "41590"
    ))
    val allPensionByCom = spark.read.jdbc(url, "CLCTDATA.NPS_JOIN_BZPL", connProp)
    //  가입상태가 탈퇴(2)인 기업 제외하고 등록(1)인 상태의 데이터만 추출 & 필요한 기간내 데이터
    //  직원수 0인 데이터 제외
    val monthSeriesPensionByComPre = allPensionByCom.
      filter('RGST_CD === "1").
      filter('NMB_SBS =!= 0).
      filter('DATE >= srvcStrtDt.toInt && 'DATE <= srvcEndDt.toInt)
    val monthSeriesPensionAmtMemberCntPre1 = monthSeriesPensionByComPre.
      select(
        'IDX,
        'DATE,
        concat('SD_CD, lit(""), 'SGG_CD).as("SGG_CODE_PRE"),
        'AMT,
        'NMB_SBS.as("MEMBER_CNT")
      )

    val monthSeriesPensionAmtMemberCntPre2 = monthSeriesPensionAmtMemberCntPre1.
      withColumn("SGG_CODE", coalesce(sggCodeExceptMapCol('SGG_CODE_PRE), 'SGG_CODE_PRE))

    val monthSeriesPensionAmtMemberCnt = monthSeriesPensionAmtMemberCntPre2.drop("SGG_CODE_PRE")

    monthSeriesPensionAmtMemberCnt
  }
}