package make_score.catering_activity_score

import common.setting_function.CmnFunc
import common.jdbc.JdbcSaveResultData
import org.apache.spark.sql.DataFrame

object RunCateringActivityScore extends java.io.Serializable {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // DB에 결과 저장
  def saveCateringActivityScore(cateringActivityScore:DataFrame) = {
    val saveJdbcResultObj = new JdbcSaveResultData
    saveJdbcResultObj.saveCateringActivityScore(cateringActivityScore)
  }

  // 요식업 활성도 지수 실행
  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunCateringActivityScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 활성도 지수 추출 시작")
    val getCateringActivityScoreObj = new GetCateringActivityScore(spark)
    val cateringActivityScorePre = getCateringActivityScoreObj.getCateringActivityScore(srvcStrtDt, srvcEndDt)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 활성도 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 활성도 평준화 지수 추출 시작")
    val cateringActivityScore = new GetStandardActivityScore(spark).getStandardActivityScore(cateringActivityScorePre)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 활성도 평준화 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 활성도 지수 DB 저장 시작")
    saveCateringActivityScore(cateringActivityScore)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 활성도 지수 DB 저장 완료")

    logger.info("[appName=AT] [function=RunCateringActivityScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}