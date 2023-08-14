/*
국민연금 근로인구, 평균급여 마트 데이터 실행

*/
package make_mart

import common.mart_data.GetSggPensionAmtPopMartData
import common.setting_function.CmnFunc

object RunSggPensionAmtPopMart {
  // spark
  val cmnFuncCls = new CmnFunc()
  val spark = cmnFuncCls.getSpark()
  val logger = cmnFuncCls.getLogger()

  def main(args: Array[String]) = {
    val (srvcStrtDt, srvcEndDt) = (args(0), args(1))
    logger.info("[appName=AT] [function=RunSggPensionAmtPopMart] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=start] [message=start]")

    println(srvcStrtDt + " ~ " + srvcEndDt + " 근로인구, 평균급여 마트 데이터 추출 시작")
    new GetSggPensionAmtPopMartData(spark).getSggPensionAmtPopMartData(srvcStrtDt, srvcEndDt)
    println(srvcStrtDt + " ~ " + srvcEndDt + " 근로인구, 평균급여 마트 데이터 추출 완료")
    logger.info("[appName=AT] [function=RunSggPensionAmtPopMart] [date="+ srvcStrtDt + " ~ " + srvcEndDt +"] [runStatus=end] [message=end]")
  }
}