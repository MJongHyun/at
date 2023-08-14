/*
배후인구지수 추출

*/
package make_score.back_population_score

import common.jdbc.{JdbcGetMartData, JdbcGetPotentialPopData, JdbcSaveResultData}
import org.apache.spark.sql.DataFrame

class GetBackPopulationScore(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  //  근로인구, 거주인구, 잠재인구 가져오기
  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    val getMartDataObj = new JdbcGetMartData(spark)
    //  근로인구 마트 데이터
    val pensionPopMart = getMartDataObj.getPensionPopMart(srvcStrtDt, srvcEndDt)
    //  거주인구 마트 데이터
    val residentPopMart = getMartDataObj.getResidentPopMart(srvcStrtDt, srvcEndDt)

    //  잠재인구 마트 데이터
    val getPotentialPopData = new JdbcGetPotentialPopData(spark)
    val potentialPopMart = getPotentialPopData.getPotentialPopData(srvcStrtDt, srvcEndDt)

    (pensionPopMart, residentPopMart, potentialPopMart)
  }

  //  근로인구 + 거주인구 + 잠재인구로 배후인구 구하기
  def getSumPop(pensionPopMart: DataFrame, residentPopMart: DataFrame, potentialPopMart:DataFrame) = {
    val pensionResident = pensionPopMart.
      join(residentPopMart, Seq("SGG_CODE", "DATE"), "fullouter")
    val allPopPre = pensionResident.join(potentialPopMart, Seq("SGG_CODE", "DATE"), "fullouter")
    val allPop = allPopPre.na.fill(0)

    val sumPopPre = allPop.withColumn("RBIZ_TOID_BHD_PUL_IDEX_RAW", 'workingPop + 'TTL_POP + 'PTNT_IDX)
    val sumPop = sumPopPre.
      select(
        'DATE.as("BASE_YM"),
        'SGG_CODE.as("SGGU_CODE"),
        'RBIZ_TOID_BHD_PUL_IDEX_RAW
      )

    sumPop
  }

  def getBackPopulationScore(srvcStrtDt: String, srvcEndDt: String) = {
    val (pensionPopMart, residentPopMart, potentialPopMart) = getSourceData(srvcStrtDt: String, srvcEndDt: String)
    val sumPop = getSumPop(pensionPopMart, residentPopMart, potentialPopMart)

    sumPop
  }
}