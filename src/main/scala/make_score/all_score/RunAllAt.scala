/*
모든 마트 데이터, 지수 실행

*/
package make_score.all_score

import make_mart.RunAllMart
import make_score.{
  back_population_score,
  catering_activity_score,
  catering_balance_score,
  catering_change_score,
  catering_density_score,
  potential_purchasing_power_score
}
import common.setting_function.CmnFunc

object RunAllAt extends java.io.Serializable {
  val cmnFuncCls = new CmnFunc()
  val logger = cmnFuncCls.getLogger()

  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunAllAt] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 필요한 마트 데이터 추출 시작")
    RunAllMart.main(args)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 필요한 마트 데이터 추출 완료")

    //  모든 지수 추출
    RunAllScore.main(args)

    logger.info("[appName=AT] [function=RunAllAt] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}