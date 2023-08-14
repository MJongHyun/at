/*
요식업 밀집도 지수 Run

*/
package make_score.catering_density_score

import common.jdbc.JdbcSaveResultData
import org.apache.spark.sql.DataFrame
import common.setting_function.CmnFunc

object RunCateringDensityScore extends java.io.Serializable {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  //  DB 결과 저장
  def saveCateringDensityScore(cateringDensityScore: DataFrame) = {
    //  DB 저장
    val saveJdbcResultObj = new JdbcSaveResultData
    saveJdbcResultObj.saveCateringDensityScore(cateringDensityScore)
  }

  //  요식업 밀집도 지수 실행
  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunCateringDensityScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"]  [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 밀집도 지수 추출 시작")

    val getCateringDensityScoreObj = new GetCateringDensityScore(spark)
    val cateringComCntByArea = getCateringDensityScoreObj.getCateringDensityScore(srvcStrtDt, srvcEndDt)

    val setCateringDensityResultFormatObj = new SetCateringDensityResultFormat(spark)
    val cateringDensityScorePre = setCateringDensityResultFormatObj.
      setCateringDensityResultFormat(srvcStrtDt, srvcEndDt, cateringComCntByArea)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 밀집도 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 밀집도 평준화 지수 추출 시작")
    val cateringDensityScore = new GetStandardCateringDensityScore(spark).
      getStandardCateringDensityScore(cateringDensityScorePre)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 밀집도 평준화 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 밀집도 지수 DB 저장 시작")
    saveCateringDensityScore(cateringDensityScore)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 밀집도 지수 DB 저장 완료")
    logger.info("[appName=AT] [function=RunCateringDensityScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"]  [runStatus=end] [message=end]")
  }
}