/*
분기별 데이터인 균형지수에서 사용하기 위한 분기 체크 및 분기 시작 년월

*/
package common.setting_function

import org.joda.time.format.DateTimeFormat

class CheckQuarterEndMonth {
//  분기 끝나는 월인지 체크
  def checkQuarterEndMonth(srvcEndDt: String) = {
    val formatter = DateTimeFormat.forPattern("yyyyMM")

    val endDate = formatter.parseDateTime(srvcEndDt)
    val endMonth = endDate.getMonthOfYear
    val checkQuarter = endMonth % 3

    if (checkQuarter == 0) true
    else false
  }
//  분기 끝나는 월이면 분기에 해당하는 시작 년월(-2월) 뽑아주기
  def getQuarterStartMonth(srvcEndDt: String) = {
    val formatter = DateTimeFormat.forPattern("yyyyMM")

    val endDate = formatter.parseDateTime(srvcEndDt)
    val startDate = endDate.minusMonths(2).toString(formatter)

    startDate
  }
}