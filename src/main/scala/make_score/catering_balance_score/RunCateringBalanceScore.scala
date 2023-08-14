package make_score.catering_balance_score

import common.setting_function.CmnFunc
import common.jdbc.JdbcSaveResultData
import org.apache.spark.sql.DataFrame

object RunCateringBalanceScore extends java.io.Serializable {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  // DB에 결과 저장
  def saveCateringBalanceScore(cateringBalanceScore:DataFrame) = {
    val saveJdbcResultObj = new JdbcSaveResultData
    saveJdbcResultObj.saveCateringBalanceScore(cateringBalanceScore)
  }

  // 요식업 균형 지수 실행
  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunCateringBalanceScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 균형 지수 추출 시작")
    val getCateringBalanceScoreObj = new GetCateringBalanceScore(spark)
    val cateringBalanceScorePre = getCateringBalanceScoreObj.getCateringBalanceScore(srvcStrtDt, srvcEndDt)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 균형 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 균형 평준화 지수 추출 시작")
    val cateringBalanceScore = new GetStandardCateringBalanceScore(spark).
      getStandardCateringBalanceScore(cateringBalanceScorePre)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 균형 평준화 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 균형 지수 DB 저장 시작")
    saveCateringBalanceScore(cateringBalanceScore)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 요식업 균형 지수 DB 저장 완료")
    logger.info("[appName=AT] [function=RunCateringBalanceScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}