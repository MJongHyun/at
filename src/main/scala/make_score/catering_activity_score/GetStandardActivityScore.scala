/*
활성도평준화지수 추출

*/
package make_score.catering_activity_score

import common.jdbc.JdbcGetMartData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class GetStandardActivityScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getStandardMaxIndex = {
    //  평준화 기준 MAX 값 추출
    val jdbcGetMartDataObj = new JdbcGetMartData(spark)
    val standardMaxIndex = jdbcGetMartDataObj.getStandardMaxIndex("TB_TS_RBAI")

    standardMaxIndex
  }

  def getStandardScore(cateringActivityScorePre: DataFrame, standardMaxIndex: DataFrame) = {
    val standardActivityScorePre1 = cateringActivityScorePre.crossJoin(standardMaxIndex)
    val standardActivityScorePre2 = standardActivityScorePre1.
      withColumn("RBIZ_TOID_ACVT_STDZ_IDEX", round(('RBIZ_TOID_ACVT_IDEX / 'MAX_IDX * 100), 2))
    val standardActivityScore = standardActivityScorePre2.drop("MAX_IDX")

    standardActivityScore
  }

  // 평준화 지수 추출
  def getStandardActivityScore(cateringActivityScorePre: DataFrame) = {
    val standardMaxIndex = getStandardMaxIndex
    val standardActivityScore = getStandardScore(cateringActivityScorePre, standardMaxIndex)

    standardActivityScore
  }
}