/*
균형 평준화지수 추출

*/
package make_score.catering_balance_score

import common.jdbc.JdbcGetMartData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class GetStandardCateringBalanceScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  def getStandardMaxIndex = {
    //  평준화 기준 MAX 값 추출
    val jdbcGetMartDataObj = new JdbcGetMartData(spark)
    val standardMaxIndex = jdbcGetMartDataObj.getStandardMaxIndex("TB_TS_RBBI")

    standardMaxIndex
  }

  def getStandardScore(cateringBalanceScorePre: DataFrame, standardMaxIndex: DataFrame) = {
    val standardCateringBalanceScorePre1 = cateringBalanceScorePre.crossJoin(standardMaxIndex)
    val standardCateringBalanceScorePre2 = standardCateringBalanceScorePre1.
      withColumn("RBIZ_TOID_BALN_STDZ_IDEX", round(('RBIZ_TOID_BALN_IDEX / 'MAX_IDX * 100), 2))
    val standardCateringBalanceScore = standardCateringBalanceScorePre2.drop("MAX_IDX")

    standardCateringBalanceScore
  }

  // 균형 평준화 지수 추출
  def getStandardCateringBalanceScore(cateringBalanceScorePre: DataFrame) = {
    val standardMaxIndex = getStandardMaxIndex
    val standardCateringBalanceScore = getStandardScore(cateringBalanceScorePre, standardMaxIndex)

    standardCateringBalanceScore
  }
}