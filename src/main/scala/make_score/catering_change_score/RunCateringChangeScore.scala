package make_score.catering_change_score

import common.setting_function.CmnFunc
import common.jdbc.JdbcSaveResultData
import org.apache.spark.sql.DataFrame

object RunCateringChangeScore extends java.io.Serializable {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // DB에 결과 저장
  def saveCateringChangeScore(cateringChangeScore:DataFrame) = {
    val saveJdbcResultObj = new JdbcSaveResultData
    saveJdbcResultObj.saveCateringChangeScore(cateringChangeScore)
  }

  // 요식업 변화율 지수 실행
  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunCateringChangeScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 변화율 지수 추출 시작")

    val getCateringChangeScoreObj = new GetCateringChangeScore(spark)
    val cateringChangeScorePre1 = getCateringChangeScoreObj.getCateringChangeScore(srvcStrtDt, srvcEndDt)

    val setCateringChangeResultFormatObj = new SetCateringChangeResultFormat(spark)
    val cateringChangeScorePre2 = setCateringChangeResultFormatObj.
      setCateringChangeResultFormat(srvcStrtDt, srvcEndDt, cateringChangeScorePre1)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 변화율 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 변화율 평준화 지수 추출 시작")
    val cateringChangeScore = new GetStandardCateringChangeScore(spark).
      getStandardCateringChangeScore(cateringChangeScorePre2)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 변화율 평준화 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 변화율 지수 DB 저장 시작")
    saveCateringChangeScore(cateringChangeScore)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 변화율 지수 DB 저장 완료")
    logger.info("[appName=AT] [function=RunCateringChangeScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}