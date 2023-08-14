/*
배후인구평준화지수 추출

*/
package make_score.back_population_score

import common.jdbc.JdbcGetMartData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class GetStandardBackPopulationScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getStandardMaxIndex = {
    //  평준화 기준 MAX 값 추출
    val jdbcGetMartDataObj = new JdbcGetMartData(spark)
    val standardMaxIndex = jdbcGetMartDataObj.getStandardMaxIndex("TB_TS_RBBPI")

    standardMaxIndex
  }

  def getStandardScore(backPopulationScorePre: DataFrame, standardMaxIndex: DataFrame) = {
    val standardBackPopulationScorePre1 = backPopulationScorePre.crossJoin(standardMaxIndex)
    val standardBackPopulationScorePre2 = standardBackPopulationScorePre1.
      withColumn("RBIZ_TOID_BHD_PUL_STDZ_IDEX", round(('RBIZ_TOID_BHD_PUL_IDEX / 'MAX_IDX * 100), 2))
    val standardBackPopulationScore = standardBackPopulationScorePre2.
      drop("MAX_IDX")

    standardBackPopulationScore
  }

  //  배후인구평준화지수 추출
  def getStandardBackPopulationScore(backPopulationScorePre: DataFrame) = {
    val standardMaxIndex = getStandardMaxIndex
    val standardBackPopulationScore = getStandardScore(backPopulationScorePre, standardMaxIndex)

    standardBackPopulationScore
  }
}