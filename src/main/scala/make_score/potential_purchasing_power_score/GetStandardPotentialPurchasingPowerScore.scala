/*
잠재구매력 평준화 지수 추출

*/
package make_score.potential_purchasing_power_score

import common.jdbc.JdbcGetMartData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class GetStandardPotentialPurchasingPowerScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getStandardMaxIndex = {
    //  평준화 기준 MAX 값 추출
    val jdbcGetMartDataObj = new JdbcGetMartData(spark)
    val standardMaxIndex = jdbcGetMartDataObj.getStandardMaxIndex("TB_TS_PPI")

    standardMaxIndex
  }

  def getStandardScore(potentialPurchasingPowerScorePre2: DataFrame, standardMaxIndex: DataFrame) = {
    val standardPotentialPurchasingPowerScorePre1 = potentialPurchasingPowerScorePre2.crossJoin(standardMaxIndex)
    val standardPotentialPurchasingPowerScorePre2 = standardPotentialPurchasingPowerScorePre1.
      withColumn("PTT_PURCP_STDZ_IDEX", round(('PTT_PURCP_IDEX / 'MAX_IDX * 100), 2))
    val standardPotentialPurchasingPowerScore = standardPotentialPurchasingPowerScorePre2.
      drop("MAX_IDX")

    standardPotentialPurchasingPowerScore
  }

  // 잠재구매력 펑준화 지수 추출
  def getStandardPotentialPurchasingPowerScore(potentialPurchasingPowerScorePre2: DataFrame) = {
    val standardMaxIndex = getStandardMaxIndex
    val standardPotentialPurchasingPowerScore = getStandardScore(potentialPurchasingPowerScorePre2, standardMaxIndex)

    standardPotentialPurchasingPowerScore
  }
}