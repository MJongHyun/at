package make_score.catering_change_score

import common.set_result_format.SetDateSggFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.round

class SetCateringChangeResultFormat  (spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  //  기준 맞춰주기 전 필요한 컬럼만
  def getCateringChangeResultPre(cateringChangeScorePre: DataFrame) = {
    val cateringChangeResultPre1 = cateringChangeScorePre.
      withColumn("changeScore_round", round('changeScore, 10))
    val cateringChangeResultPre2 = cateringChangeResultPre1.
      select(
        'SGG_CODE.as("SGGU_CODE"),
        'DATE.as("BASE_YM"),
        'changeScore_round.as("RBIZ_TOID_CHG_RT_IDEX")
      )

    cateringChangeResultPre2
  }

  def setCateringChangeResultFormat(srvcStrtDt: String, srvcEndDt: String, cateringChangeScorePre: DataFrame) = {
    val cateringChangeResultPre = getCateringChangeResultPre(cateringChangeScorePre)

    val setDateSggFormatObj = new SetDateSggFormat(spark)
    val cateringChangeScore = setDateSggFormatObj.
      setDateSggFormat(srvcStrtDt, srvcEndDt, cateringChangeResultPre)

    cateringChangeScore
  }
}