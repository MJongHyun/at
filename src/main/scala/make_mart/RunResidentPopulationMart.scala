/*
거주인구 마트 데이터 실행

*/
package make_mart

import common.mart_data.GetResidentPopulationMartData
import common.setting_function.CmnFunc

object RunResidentPopulationMart {
  // spark
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunResidentPopulationMart] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 거주인구 마트 데이터 추출 시작")
    new GetResidentPopulationMartData(spark).getResidentPopulationMartData(srvcStrtDt, srvcEndDt)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 거주인구 마트 데이터 추출 완료")
    logger.info("[appName=AT] [function=RunResidentPopulationMart] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}