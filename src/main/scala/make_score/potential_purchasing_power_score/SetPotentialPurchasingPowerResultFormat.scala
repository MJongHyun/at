/*
잠재구매력 지수: 시군구, 날짜 누락된 데이터 없도록 0인 값 채워주기

*/
package make_score.potential_purchasing_power_score

import common.set_result_format.SetDateSggFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class SetPotentialPurchasingPowerResultFormat(spark: org.apache.spark.sql.SparkSession) {

  import spark.implicits._

  def setBackPopulationRoundResult(potentialPurchasingPowerScorePre1: DataFrame) = {
    val potentialPurchasingPowerScorePre2 = potentialPurchasingPowerScorePre1.
      withColumn("PTT_PURCP_IDEX", round('PTT_PURCP_IDEX_RAW, 10))
    val potentialPurchasingPowerScore = potentialPurchasingPowerScorePre2.drop("PTT_PURCP_IDEX_RAW")

    potentialPurchasingPowerScore
  }
  def setPotentialPurchasingPowerResultFormat(srvcStrtDt: String,
                                              srvcEndDt: String,
                                              potentialPurchasingPowerScorePre: DataFrame) = {
    val setDateSggFormatObj = new SetDateSggFormat(spark)
    val potentialPurchasingPowerScorePre1 = setDateSggFormatObj.
      setDateSggFormat(srvcStrtDt, srvcEndDt, potentialPurchasingPowerScorePre)
    val potentialPurchasingPowerScore = setBackPopulationRoundResult(potentialPurchasingPowerScorePre1)

    potentialPurchasingPowerScore
  }
}