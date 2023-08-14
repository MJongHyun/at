/*
변화율 평준화 지수 추출
*/
package make_score.catering_change_score

import common.jdbc.JdbcGetMartData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class GetStandardCateringChangeScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getStandardMaxIndex = {
    //  평준화 기준 MAX 값 추출
    val jdbcGetMartDataObj = new JdbcGetMartData(spark)
    val standardMaxIndex = jdbcGetMartDataObj.getStandardMaxIndex("TB_TS_RBCRI")

    standardMaxIndex
  }

  def getStandardScore(cateringChangeScorePre2: DataFrame, standardMaxIndex: DataFrame) = {
    val standardCateringChangeScorePre1 = cateringChangeScorePre2.crossJoin(standardMaxIndex)
    val standardCateringChangeScorePre2 = standardCateringChangeScorePre1.
      withColumn("RBIZ_TOID_CHG_RT_STDZ_IDEX", round(('RBIZ_TOID_CHG_RT_IDEX / 'MAX_IDX * 100), 2))
    val standardCateringChangeScore = standardCateringChangeScorePre2.drop("MAX_IDX")

    standardCateringChangeScore
  }

  // 변화율 평준화 지수 추출
  def getStandardCateringChangeScore(cateringChangeScorePre2: DataFrame) = {
    val standardMaxIndex = getStandardMaxIndex
    val standardCateringChangeScore = getStandardScore(cateringChangeScorePre2, standardMaxIndex)

    standardCateringChangeScore
  }
}