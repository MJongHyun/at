package make_score.catering_change_score

import common.jdbc.JdbcGetMartData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

class GetCateringChangeScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    // 변화율 지수를 추출하기 위한 전년도 값 추출
    val nowYear = srvcStrtDt.slice(0, 4)
    val nowMonth = srvcStrtDt.slice(4, 6)
    val bfYear = (nowYear.toInt - 1).toString
    val bfDate = bfYear + nowMonth
    // 지방인허가 데이터 요식업종별 시군구별 업체수 데이터
    val getMartDataObj = new JdbcGetMartData(spark)
    val cateringSggComCntData = getMartDataObj.getCateringSggComCntData(bfDate, srvcEndDt)

    cateringSggComCntData
  }
  // 지난해 연월 값을 추출하여 변화율 지수 추출
  def getNowBfYearCateringCntData(
                                   cateringSggComCntData: DataFrame,
                                   srvcStrtDt: String,
                                   srvcEndDt: String) = {
    // 연월대비 지난해 연월 값 함수
    val getBfFuc: (String) => String = {
      (nowDate) =>
        val nowYear = nowDate.slice(0, 4)
        val nowMonth = nowDate.slice(4, 6)
        val bfYear = (nowYear.toInt - 1).toString
        val bfDate = bfYear + nowMonth
        bfDate
    }
    val getBfDateUdf: UserDefinedFunction = udf(getBfFuc)
    // 연월대비 시군구별 요식업 업체 수 추출
    val cateringSggCnt = cateringSggComCntData.
      groupBy("DATE", "SGG_CODE").
      count
    // 현재 시군구별 요식업 업체 수 추출
    val nowCateringSggCntPre1 = cateringSggCnt.
      withColumnRenamed("count", "nowCnt").
      withColumn("bfDate", getBfDateUdf('DATE))
    val nowCateringSggCnt = nowCateringSggCntPre1.
      filter('DATE >= srvcStrtDt.toInt)
    // 작년 시군구별 요식업 업체 수 추출
    val lastYm = getBfFuc(srvcEndDt)
    val bfCateringSggCntPre1 = cateringSggCnt.
      withColumnRenamed("DATE", "bfDate").
      withColumnRenamed("count", "bfCnt")
    val bfCateringSggCnt = bfCateringSggCntPre1.
      filter('bfDate <= lastYm)
    // 현재,작년 시군구별 요식업 업체 수 추출
    val nowBfCateringSggCntPre1 = nowCateringSggCnt.
      join(bfCateringSggCnt, Seq("SGG_CODE", "bfDate"), "outer")
    val nowBfCateringSggCnt = nowBfCateringSggCntPre1.
    na.fill(0)
    // 변화율 지수 추출
    val changeScoreData = nowBfCateringSggCnt.
        withColumn("changeScore", when('bfCnt === 0, 'nowCnt).
          otherwise(('nowCnt - 'bfCnt)/'bfCnt))

    changeScoreData
    }
  //  요식업종 변화율 지수 구하기
  def getCateringChangeScore(srvcStrtDt:String, srvcEndDt:String) = {
    // 시군구 코드 데이터, 지방인허가 시군구별, 업종별, 업체수 데이터 저장
    val cateringSggComCntData = getSourceData(srvcStrtDt, srvcEndDt)
    // 변화율 지수 추출
    val cateringChangeScorePre = getNowBfYearCateringCntData(cateringSggComCntData, srvcStrtDt, srvcEndDt)

    cateringChangeScorePre
  }
}