/*
make ResidentPopulation mart data

*/
package common.mart_data

import common.jdbc.{JdbcGetPublicData, JdbcSaveMartData}
import org.apache.spark.sql.DataFrame

class GetResidentPopulationMartData(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  //  거주 인구 가져오기
  def getSourceData(srvcStrtDt: String, srvcEndDt: String) = {
    val jdbcGetPublicDataObj = new JdbcGetPublicData(spark)
    val residentPopulation = jdbcGetPublicDataObj.
      getSggMonthSeriesResidentPopulation(srvcStrtDt, srvcEndDt)

    residentPopulation
  }

  //  거주 인구 마트 저장
  def saveResidentPopulationMartData(residentPopulation: DataFrame) = {
    val saveMartDataObj = new JdbcSaveMartData
    saveMartDataObj.saveResidentPopulationMartData(residentPopulation)
  }

  //  거주인구 마트 만들기
  def getResidentPopulationMartData(srvcStrtDt: String, srvcEndDt: String) = {
    val residentPopulation = getSourceData(srvcStrtDt, srvcEndDt)

    saveResidentPopulationMartData(residentPopulation)
  }
}