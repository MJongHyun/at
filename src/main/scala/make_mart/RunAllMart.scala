/*
모든 마트 데이터 실행

*/
package make_mart

import common.setting_function.CmnFunc

object RunAllMart {
  val cmnFuncCls = new CmnFunc()
  val logger = cmnFuncCls.getLogger()

  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunAllMart] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    //  지방인허가 요식업 마트
    RunCateringMart.main(args)
    //  거주 인구 마트
    RunResidentPopulationMart.main(args)
    //  국민연금 근로인구, 평균급여 마트
    RunSggPensionAmtPopMart.main(args)
    logger.info("[appName=AT] [function=RunAllMart] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}