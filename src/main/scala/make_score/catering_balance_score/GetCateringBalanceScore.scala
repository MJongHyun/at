package make_score.catering_balance_score

import common.jdbc.{JdbcGetCollectData, JdbcGetMartData}
import common.set_result_format.SetCateringDateQuarterSggFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

class GetCateringBalanceScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getSourceData(srvcStrtDt:String, srvcEndDt:String) = {
    // 시도, 시군구 코드 추출
    val JdbcGetCollectDataObj = new JdbcGetCollectData(spark)
    val areaCodeData = JdbcGetCollectDataObj.getAreaCode
    // 지방인허가 데이터 요식업종별 시군구별 업체수 데이터
    val getMartDataObj = new JdbcGetMartData(spark)
    val cateringSggComCntData = getMartDataObj.getCateringSggComCntData(srvcStrtDt, srvcEndDt)
    // 월별 분기별, 시도, 시군구, 요식업소분류 데이터 추출하여 마트데이터 추출
    val getCateringQuarterSggFormatObj  = new SetCateringDateQuarterSggFormat(spark)
    val cateringSggComCntMartData = getCateringQuarterSggFormatObj.
      setCateringDateQuarterSggFormat(srvcStrtDt, srvcEndDt, cateringSggComCntData)

    (cateringSggComCntMartData, areaCodeData)
  }
  // 분석연월 시군구 요식업 대비 요식업 소분류 업체 수에 대한 비율데이터 추출
  def getSggComCateringRatioData(cateringSggComCntMartData:DataFrame)  = {
    // 분석연월 시군구 요식업 총 업체 수 추출, 전체갯수가 0인 경우, 요식업 업체수가 존재하지 않으므로 비율 값을 0으로 나올 수 있게 값을 1로 대체
    val cateringSggSumDataPre1 = cateringSggComCntMartData.
      groupBy("DATE", "SGG_CODE").
      agg(sum('comCount).as("CateringTotalCntPre"))
    val cateringSggSumData = cateringSggSumDataPre1.
      withColumn("CateringTotalCnt" , when('CateringTotalCntPre === 0, 1).
        otherwise('CateringTotalCntPre)).
        select("DATE",
        "SGG_CODE",
          "CateringTotalCnt")
    // 분석연월 시군구 요식업 업체수 대비 소분류 업체 수 비율 추출
    val cateringRatioDataPre1 = cateringSggComCntMartData.
      join(cateringSggSumData, Seq("DATE", "SGG_CODE"))
    val cateringRatioDataPre2 = cateringRatioDataPre1.
      withColumn("ratioPre", ('comCount / 'CateringTotalCnt) * 10000)
    val cateringRatioDataPre3 = cateringRatioDataPre2.
      withColumn("ratio", ('ratioPre).cast(DecimalType(10,2)))
    val cateringRatioData = cateringRatioDataPre3.
      select("DATE",
        "quarter",
        "year",
        "SIDO_CODE",
        "SGG_CODE",
        "SIDO",
        "SGG",
        "mainCategory",
        "opnSvcNm",
        "uptaeNm",
        "ratio")

    cateringRatioData
  }
  // 분석연월 시군구 요식업 소분류 업체 대비 상대 비교값 추출
  def getRelativeComparisonData(cateringRatioData:DataFrame, areaCodeData:DataFrame) = {
    // 분석연월 요식업 소분류에 대한 비율 합 추출
    val cateringRatioSumData = cateringRatioData.
      groupBy("DATE","mainCategory", "opnSvcNm", "uptaeNm").
      agg(sum('ratio).as("sumRatio"))
    // 분석연월 요식업에 종사하는 시군구 추출, 상대비교 평균을 구할 때 자신 - 평균이기 때문에 (시군구 수 - 1 값을 추출)
    val cateringSggCnt = areaCodeData.count
    val relativeComparisonDataPre1 = cateringRatioData.
      join(cateringRatioSumData, Seq("DATE", "mainCategory", "opnSvcNm", "uptaeNm"))
    val relativeComparisonDataPre2 = relativeComparisonDataPre1.
      withColumn("SGG_CN", lit(cateringSggCnt) - 1)
    // 위의 값 추출 후, 상대비교 값 추출
    val relativeComparisonDataPre3 = relativeComparisonDataPre2.
      withColumn("relMeanRatioPre", ('sumRatio - 'ratio)/'SGG_CN)
    val relativeComparisonDataPre4 = relativeComparisonDataPre3.
      withColumn("relMeanRatio", 'relMeanRatioPre.cast(DecimalType(10,2)))
    val relativeComparisonDataPre5 = relativeComparisonDataPre4.
      withColumn("relativeComparison",'ratio - 'relMeanRatio)
    val relativeComparisonData = relativeComparisonDataPre5.
      select("DATE",
        "quarter",
        "year",
        "SIDO_CODE",
        "SGG_CODE",
        "SIDO",
        "SGG",
        "mainCategory",
        "opnSvcNm",
        "uptaeNm",
        "relativeComparison")

    relativeComparisonData
  }
  // 분석연월 시군구 요식업 소분류 업체 상대 비교값으로 변동계수 값 추출 후 균형지수 추출
  def getCoeffVarData(relativeComparisonData:DataFrame)= {
    // 시군구 대비 평균값 추출
    val meanDataPre1 = relativeComparisonData.
          groupBy("DATE", "SGG_CODE").
          agg(avg('relativeComparison) as "meanPre1")
    // 값이 소수점 6자리라서 값 10000 곱한 후, 소수 둘째자리까지 자름
    val meanDataPre2 = meanDataPre1.
      withColumn("meanPre2", 'meanPre1 * 10000)
    val meanData = meanDataPre2.
      withColumn("mean", 'meanPre2.cast(DecimalType(10,2))).
      select('Date,
        'SGG_CODE,
        'mean)
    // 시군구 대비 표준편차 추출
    val stdDataPre1 = relativeComparisonData.
      groupBy("DATE", "SGG_CODE").
      agg(stddev('relativeComparison) as "stdPre1")
    // 똑같이 10000 곱한 후, 소수 둘째자리까지 자름
    val stdDataPre2 = stdDataPre1.
      withColumn("stdPre2", 'stdPre1 * 10000)
    val stdDataPre3 = stdDataPre2.
      withColumn("stdPre3", ('stdPre2).cast(DecimalType(10,2)))
    val stdData = stdDataPre3.
      withColumn("std", when('stdPre3 === 0, 0.01).
        otherwise('stdPre3)).
      select('Date,
        'SGG_CODE,
        'std)
    // DB의 속성과 일치하도록 균형지수 추출
    val martData = relativeComparisonData.
      select("DATE",
        "year",
        "quarter",
        "SIDO_CODE",
        "SGG_CODE",
        "SIDO",
        "SGG").
      distinct()
    // 변동계수 역수를 구한 후, 분기별 평균을 구하여 균형지수 추출
    val coeffVarMartDataPre1 = meanData.
      join(stdData, Seq("DATE", "SGG_CODE"))
    val coeffVarMartDataPre2 = coeffVarMartDataPre1.
      withColumn("coeffVarPre1", ('mean/'std) * 1000000)
    val coeffVarMartDataPre3 = coeffVarMartDataPre2.
      withColumn("coeffVar", 'coeffVarPre1.cast(DecimalType(10,2)))
    val coeffVarMartData = coeffVarMartDataPre3.
      join(martData, Seq("DATE", "SGG_CODE"))
    val cateringBalanceScorePre1 = coeffVarMartData.
      groupBy("year", "quarter", "SGG_CODE").
      agg(avg('coeffVar) as "balanceScorePre1")
    val cateringBalanceScorePre2 = cateringBalanceScorePre1.
      withColumn("balanceScore", 'balanceScorePre1.cast(DecimalType(10,2)))
    val cateringBalanceScorePre3 = cateringBalanceScorePre2.
      join(martData, Seq("year", "quarter", "SGG_CODE"))
    val cateringBalanceScore = cateringBalanceScorePre3.
      select(
        'DATE.as("BASE_YM"),
        'quarter.as("BASE_QU_CODE"),
        'SIDO_CODE.as("CTNP_CODE"),
        'SGG_CODE.as("SGGU_CODE"),
        'SIDO.as("CTNP_NM"),
        'SGG.as("SGGU_NM"),
        'balanceScore.as("RBIZ_TOID_BALN_IDEX")
      )


    cateringBalanceScore
  }

  //  요식업종 균형지수 구하기
  def getCateringBalanceScore(srvcStrtDt:String, srvcEndDt:String) = {
    // 시군구 코드 데이터, 균형지수를 추출하기 위한 마트데이터 추출
    val (cateringSggComCntMartData, areaCodeData) = getSourceData(srvcStrtDt, srvcEndDt)
    // 시군구 요식업 대비 요식업 소분류 업체 수에 대한 비율데이터 추출
    val cateringRatioData = getSggComCateringRatioData(cateringSggComCntMartData)
    // 시군구 요식업 소분류 업체 대비 상대 비교값 추출
    val relativeComparisonData = getRelativeComparisonData(cateringRatioData, areaCodeData)
    // 시군구 요식업 소분류 업체 상대 비교값으로 변동계수 값 추출 후 균형지수 추출
    val cateringBalanceScorePre = getCoeffVarData(relativeComparisonData)

    cateringBalanceScorePre
  }
}