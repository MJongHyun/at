/*
분석 결과 format 맞춰주기 위해 분석날짜에 해당하는 모든 년월과 분기 추출

*/
package common.set_result_format

import org.joda.time.Months
import org.joda.time.format.DateTimeFormat

class GetDateQuarterFormat(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getMonthQuarterList(srvcStrtDt: String, srvcEndDt: String) = {
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
      yield (
          startDate.plusMonths(month).getYear,
          startDate.plusMonths(month).toString(formatter).toInt,
          (startDate.plusMonths(month).getMonthOfYear - 1)/3+1
        )

    val monthList = monthVec.toList

    monthList
  }

  def getMonthQuarterDataFrame(monthList: List[(Int, Int, Int)]) = {
    val monthDf = monthList.toDF("year", "DATE", "quarter")

    monthDf
  }

  def getDateQuarterFormat(srvcStrtDt: String, srvcEndDt: String) = {
    val monthQuaterList = getMonthQuarterList(srvcStrtDt, srvcEndDt)
    val monthQuaterDf = getMonthQuarterDataFrame(monthQuaterList)

    monthQuaterDf
  }
}