/*
배후 인구 지수: 시군구, 날짜 누락된 데이터 없도록 0인 값 채워주기

*/
package make_score.back_population_score

import common.set_result_format.SetDateSggFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class SetBackPopulationResultFormat(spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  def setBackPopulationRoundResult(backPopulationScorePre1: DataFrame) = {
    val backPopulationScorePre2 = backPopulationScorePre1.
      withColumn("RBIZ_TOID_BHD_PUL_IDEX", round('RBIZ_TOID_BHD_PUL_IDEX_RAW, 10))
    val backPopulationScorePre = backPopulationScorePre2.drop("RBIZ_TOID_BHD_PUL_IDEX_RAW")

    backPopulationScorePre
  }

  def setBackPopulationResultFormat(srvcStrtDt: String,
                                    srvcEndDt: String,
                                    sumPop: DataFrame) = {
    val setDateSggFormatObj = new SetDateSggFormat(spark)
    val backPopulationScorePre1 = setDateSggFormatObj.
      setDateSggFormat(srvcStrtDt, srvcEndDt, sumPop)
    val backPopulationScorePre = setBackPopulationRoundResult(backPopulationScorePre1)

    backPopulationScorePre
  }
}