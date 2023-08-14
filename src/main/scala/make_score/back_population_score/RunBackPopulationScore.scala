/*
배후인구지수 실행

*/
package make_score.back_population_score

import common.jdbc.JdbcSaveResultData
import org.apache.spark.sql.DataFrame
import common.setting_function.CmnFunc

object RunBackPopulationScore extends java.io.Serializable {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  //  결과 DB 저장
  def saveBackPopulationScore(sumPop: DataFrame) = {
    val saveJdbcResultObj = new JdbcSaveResultData
    saveJdbcResultObj.saveBackPopulationScore(sumPop)
  }

  //  배후인구 지수 실행
  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))

    logger.info("[appName=AT] [function=RunBackPopulationScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 배후인구 지수 추출 시작")
    val getBackPopulationScoreObj = new GetBackPopulationScore(spark)
    val sumPop = getBackPopulationScoreObj.getBackPopulationScore(srvcStrtDt, srvcEndDt)

    val setBackPopulationResultFormatObj = new SetBackPopulationResultFormat(spark)
    val backPopulationScorePre = setBackPopulationResultFormatObj.
      setBackPopulationResultFormat(srvcStrtDt, srvcEndDt, sumPop)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 배후인구 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 배후인구 평준화 지수 추출 시작")
    val backPopulationScore = new GetStandardBackPopulationScore(spark).
      getStandardBackPopulationScore(backPopulationScorePre)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 배후인구 평준화 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 배후인구 지수 DB 저장 시작")
    saveBackPopulationScore(backPopulationScore)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 배후인구 지수 DB 저장 완료")

    logger.info("[appName=AT] [function=RunBackPopulationScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}