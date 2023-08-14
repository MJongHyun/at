/*
Jdbc Save MartData

*/
package common.jdbc

import org.apache.spark.sql.{DataFrame, SaveMode}

class JdbcSaveMartData {

  val (url, connProp) = martDataJdbcProp()

  //  지방인허가 요식업 마트 데이터 저장
  def saveCateringMartData(cateringMartData: DataFrame) = {
    cateringMartData.
      write.
      mode(SaveMode.Append).
      jdbc(url, "CATERING_MRT", connProp)
  }

  //  지방인허가 요식업 마트 -> 시군구별, 업종별, 업체수 데이터 저장
  def saveCateringSggComCntData(cateringSggComCntData: DataFrame) = {
    cateringSggComCntData.
      write.
      mode(SaveMode.Append).
      jdbc(url, "CATERING_SGG_COM_CNT", connProp)
  }

  //  시군구별 평균 급여, 근로인구 마트 저장
  def saveSggPensionAmtPopMartData(pensionAmtPopMartData: DataFrame) = {
    pensionAmtPopMartData.
      write.
      mode(SaveMode.Append).
      jdbc(url, "PENSION_AMT_POP", connProp)
  }

  //  시군구별 거주 인구 수 저장
  def saveResidentPopulationMartData(residentPopulationMartData: DataFrame) = {
    residentPopulationMartData.
      write.
      mode(SaveMode.Append).
      jdbc(url, "RESIDENT_POP", connProp)
  }

  //  주소 정제 시군구 코드 값
  def saveAddrSggCodeData(addrSggCodeData: DataFrame) = {
    addrSggCodeData.
      write.
      mode(SaveMode.Append).
      jdbc(url, "ADDR_SGG", connProp)
  }
}