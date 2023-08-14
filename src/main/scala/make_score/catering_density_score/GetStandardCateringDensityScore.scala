/*
밀집도 평준화 지수 추출

*/
package make_score.catering_density_score

import common.jdbc.JdbcGetMartData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class GetStandardCateringDensityScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

    import spark.implicits._

    def getStandardMaxIndex = {
      //  평준화 기준 MAX 값 추출
      val jdbcGetMartDataObj = new JdbcGetMartData(spark)
      val standardMaxIndex = jdbcGetMartDataObj.getStandardMaxIndex("TB_TS_RBDI")

      standardMaxIndex
    }

    def getStandardScore(cateringDensityScorePre: DataFrame, standardMaxIndex: DataFrame) = {
      val standardCateringDensityScorePre1 = cateringDensityScorePre.crossJoin(standardMaxIndex)
      val standardCateringDensityScorePre2 = standardCateringDensityScorePre1.
        withColumn("RBIZ_TOID_DST_STDZ_IDEX", round(('RBIZ_TOID_DST_IDEX / 'MAX_IDX * 100), 2))
      val standardCateringDensityScore = standardCateringDensityScorePre2.drop("MAX_IDX")

      standardCateringDensityScore
    }

    // 밀집도 평준화 지수 추출
    def getStandardCateringDensityScore(cateringDensityScorePre: DataFrame) = {
      val standardMaxIndex = getStandardMaxIndex
      val standardCateringDensityScore = getStandardScore(cateringDensityScorePre, standardMaxIndex)

      standardCateringDensityScore
    }
}