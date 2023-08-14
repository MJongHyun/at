// 잠재구매력 지수 실행
package make_score.potential_purchasing_power_score

import common.jdbc.JdbcSaveResultData
import org.apache.spark.sql.DataFrame
import common.setting_function.CmnFunc

object RunPotentialPurchasingPowerScore extends java.io.Serializable {
  // spark, logger
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  //  DB 결과 저장
  def savePotentialPurchasingPowerScore(potentialPurchasingPowerScore: DataFrame) = {
    val saveJdbcResultObj = new JdbcSaveResultData
    saveJdbcResultObj.savePotentialPurchasingPowerScore(potentialPurchasingPowerScore)
  }

  // 잠재구매력 지수 실행
  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunPotentialPurchasingPowerScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 잠재구매력 지수 추출 시작")

    val getPotentialPurchasingPowerScoreObj = new GetPotentialPurchasingPowerScore(spark)
    val potentialPurchasingPowerScorePre1 = getPotentialPurchasingPowerScoreObj.
      getPotentialPurchasingPowerScore(srvcStrtDt, srvcEndDt)

    val setPotentialPurchasingPowerResultFormatObj = new SetPotentialPurchasingPowerResultFormat(spark)
    val potentialPurchasingPowerScorePre2 = setPotentialPurchasingPowerResultFormatObj.
      setPotentialPurchasingPowerResultFormat(srvcStrtDt, srvcEndDt, potentialPurchasingPowerScorePre1)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 잠재구매력 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 잠재구매력 평준화 지수 추출 시작")
    val potentialPurchasingPowerScore = new GetStandardPotentialPurchasingPowerScore(spark).
      getStandardPotentialPurchasingPowerScore(potentialPurchasingPowerScorePre2)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 잠재구매력 평준화 지수 추출 완료")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 잠재구매력 지수 DB 저장 시작")
    savePotentialPurchasingPowerScore(potentialPurchasingPowerScore)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 잠재구매력 지수 DB 저장 완료")
    logger.info("[appName=AT] [function=RunPotentialPurchasingPowerScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}