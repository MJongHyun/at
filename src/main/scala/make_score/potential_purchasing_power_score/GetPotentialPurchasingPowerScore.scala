/*
잠재 구매력 지수 추출

*/
package make_score.potential_purchasing_power_score

import common.jdbc.JdbcGetMartData
import org.apache.spark.sql.DataFrame

class GetPotentialPurchasingPowerScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  //  국민연금 데이터 가져오기
  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    val getMartDataObj = new JdbcGetMartData(spark)
    val pensionAmtMart = getMartDataObj.getPensionAmtMart(srvcStrtDt, srvcEndDt)

    pensionAmtMart
  }

  def getPotentialPurchasingPower(pensionAmtMart: DataFrame) = {
    val potentialPurchasingPowerScore = pensionAmtMart.
      select(
        'DATE.as("BASE_YM"),
        'SGG_CODE.as("SGGU_CODE"),
        'salaryAverage.as("PTT_PURCP_IDEX_RAW")
      )

    potentialPurchasingPowerScore
  }

  def getPotentialPurchasingPowerScore(srvcStrtDt: String, srvcEndDt: String) = {
    val pensionAmtMart = getSourceData(srvcStrtDt, srvcEndDt)
    val potentialPurchasingPowerScorePre = getPotentialPurchasingPower(pensionAmtMart)

    potentialPurchasingPowerScorePre
  }
}