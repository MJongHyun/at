/*
Jdbc Save score result Data

*/
package common.jdbc

import org.apache.spark.sql.{DataFrame, SaveMode}

class JdbcSaveResultData {

  val (url, connProp) = scoreJdbcPop()

  //  요식업 밀집도 지수 저장
  def saveCateringDensityScore(cateringComCntByArea: DataFrame) = {
    cateringComCntByArea.
      write.
      mode(SaveMode.Append).
      jdbc(url, "TB_TS_RBDI", connProp)
  }

  //  배후인구 지수 저장
  def saveBackPopulationScore(backPopulationScore: DataFrame) = {
    backPopulationScore.
      write.
      mode(SaveMode.Append).
      jdbc(url, "TB_TS_RBBPI", connProp)
  }

  // 잠재구매력 지수 저장
  def savePotentialPurchasingPowerScore(potentialPurchasingPowerScore: DataFrame) = {
    potentialPurchasingPowerScore.
      write.
      mode(SaveMode.Append).
      jdbc(url, "TB_TS_PPI", connProp)
  }

  // 활성도 지수 저장
  def saveCateringActivityScore(cateringActivityScore: DataFrame) = {
    cateringActivityScore.
      write.
      mode(SaveMode.Append).
      jdbc(url, "TB_TS_RBAI", connProp)
  }

  // 균형 지수 저장
  def saveCateringBalanceScore(cateringBalanceScore: DataFrame) = {
    cateringBalanceScore.
      write.
      mode(SaveMode.Append).
      jdbc(url, "TB_TS_RBBI", connProp)
  }

  // 변화율 지수 저장
  def saveCateringChangeScore(cateringChangeScore: DataFrame) = {
    cateringChangeScore.
      write.
      mode(SaveMode.Append).
      jdbc(url, "TB_TS_RBCRI", connProp)
  }
}