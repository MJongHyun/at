/*
분석 결과 format 맞춰주기 위해 분석날짜에 해당하는 모든 년월 추출

*/
package common.set_result_format

import org.joda.time.Months
import org.joda.time.format.DateTimeFormat

class GetDateFormat(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getMonthList(srvcStrtDt: String, srvcEndDt: String) = {
    val formatter = DateTimeFormat.forPattern("yyyyMM")

    val startDate = formatter.parseDateTime(srvcStrtDt)
    val endDate = formatter.parseDateTime(srvcEndDt)
    val monthsBetween = Months.monthsBetween(startDate, endDate)
    val monthsRange = monthsBetween.
      toString.
      replace("P", "").
      replace("M", "").
      toInt

    val monthVec = for (month <- 0 to monthsRange)
      yield startDate.plusMonths(month).toString(formatter)

    val monthList = monthVec.toList

    monthList
  }

  def getMonthDataFrame(monthList: List[String]) = {
    val monthDf = monthList.toDF("DATE")

    monthDf
  }

  def getDateFormat(srvcStrtDt: String, srvcEndDt: String) = {
    val monthList = getMonthList(srvcStrtDt, srvcEndDt)
    val monthDf = getMonthDataFrame(monthList)

    monthDf
  }
}