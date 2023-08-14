/*
모든 지수 실행

*/
package make_score.all_score

import make_score.{
  back_population_score,
  catering_activity_score,
  catering_balance_score,
  catering_change_score,
  catering_density_score,
  potential_purchasing_power_score
}
import common.setting_function.{CheckQuarterEndMonth, CmnFunc}

object RunAllScore extends java.io.Serializable {
  val cmnFuncCls = new CmnFunc()
  val logger = cmnFuncCls.getLogger()

  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunAllScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 지수 추출 시작")
    back_population_score.RunBackPopulationScore.main(args)
    catering_activity_score.RunCateringActivityScore.main(args)
    catering_change_score.RunCateringChangeScore.main(args)
    catering_density_score.RunCateringDensityScore.main(args)
    potential_purchasing_power_score.RunPotentialPurchasingPowerScore.main(args)
    // 분기 끝나는 월인지 확인 후, 분기 데이터인 균형지수 추출
    val CheckQuarterEndMonthObj = new CheckQuarterEndMonth
    if (CheckQuarterEndMonthObj.checkQuarterEndMonth(srvcEndDt)) {
      val startDate = CheckQuarterEndMonthObj.getQuarterStartMonth(srvcEndDt)
      println(srvcEndDt + " 가 분기 종료 월이라서 " + startDate + " 부터 균형 지수 추출")
      val balanceScoreArgs = Array(startDate, srvcEndDt)
      println(balanceScoreArgs(0), balanceScoreArgs(1))
      catering_balance_score.RunCateringBalanceScore.main(balanceScoreArgs)
    } else {
      println(srvcEndDt + " 가 분기 종료 월이 아니라서 균형 지수 추출하지 않음")
    }
    println(srvcStrtDt + " ~ " + srvcEndDt + " 지수 추출 완료")

    logger.info("[appName=AT] [function=RunAllScore] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}