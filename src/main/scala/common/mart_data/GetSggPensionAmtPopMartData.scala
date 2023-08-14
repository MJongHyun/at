/*
make sgg WorkingPopulation +  SalaryAverage mart data

*/
package common.mart_data

import common.jdbc.{JdbcGetPublicData, JdbcSaveMartData}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, expr, sum}

class GetSggPensionAmtPopMartData(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    //  국민연금데이터 가져오기
    val jdbcGetPublicDataObj = new JdbcGetPublicData(spark)
    val pensionAmtMemberCnt = jdbcGetPublicDataObj.
      getMonthSeriesPensionAmtMemberCnt(srvcStrtDt, srvcEndDt)

    pensionAmtMemberCnt
  }

  //  국민연금 납부액으로 업체 평균 급여 금액 데이터 가져오기
  def getSalaryAverageData(pensionAmtMemberCnt: DataFrame) = {
    val addSalaryAverageData = pensionAmtMemberCnt.
      select(
        'IDX,
        'DATE,
        'SGG_CODE,
        expr("AMT/MEMBER_CNT*100/9").as("SlryAvrg"),
        'MEMBER_CNT
      )

    addSalaryAverageData
  }

  //  시군구별 근로자수, 평균 급여 구하기
  def getSggPensionAmtPop(addSalaryAverageData: DataFrame) = {
    val sggPensionAmtPop = addSalaryAverageData.
      groupBy("DATE", "SGG_CODE").
      agg(
        avg("SlryAvrg").as("salaryAverage"),
        sum("MEMBER_CNT").as("workingPop")
      )

    sggPensionAmtPop
  }

  //  업체별 평균 급여 마트 저장
  def saveSggPensionAmtPopMartData(sggPensionAmtPop: DataFrame) = {
    val saveMartDataObj = new JdbcSaveMartData
    saveMartDataObj.saveSggPensionAmtPopMartData(sggPensionAmtPop)
  }

  def getSggPensionAmtPopMartData(srvcStrtDt: String, srvcEndDt: String) = {
    val pensionAmtMemberCnt = getSourceData(srvcStrtDt, srvcEndDt)
    val addSalaryAverageData = getSalaryAverageData(pensionAmtMemberCnt)
    val sggPensionAmtPop = getSggPensionAmtPop(addSalaryAverageData)

    saveSggPensionAmtPopMartData(sggPensionAmtPop)
  }
}