package make_score.catering_activity_score

import common.set_result_format.SetDateSggFormat
import org.apache.spark.sql.DataFrame

class SetCateringActivityResultFormat (spark: org.apache.spark.sql.SparkSession) {
  import spark.implicits._

  //  기준 맞춰주기 전 필요한 컬럼만
  def getCateringActivityResultPre(cateringActivityScore: DataFrame) = {
    val cateringActivityResultPre = cateringActivityScore.
      select(
        'SGG_CODE.as("SGGU_CODE"),
        'DATE.as("BASE_YM"),
        'sggComCnt,
        'TTL_POP,
        'workingPop
      )

    cateringActivityResultPre
  }

  def setCateringActivityResultFormat(srvcStrtDt: String, srvcEndDt: String, cateringActivityScorePre: DataFrame) = {

    val cateringActivityResultPre = getCateringActivityResultPre(cateringActivityScorePre)

    val setDateSggFormatObj = new SetDateSggFormat(spark)
    val cateringActivityScore = setDateSggFormatObj.
      setDateSggFormat(srvcStrtDt, srvcEndDt, cateringActivityResultPre)

    cateringActivityScore
  }
}