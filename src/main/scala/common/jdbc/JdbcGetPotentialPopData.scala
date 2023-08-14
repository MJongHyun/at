/*
98번 서버 DB에서 인구지수 중 잠재지수에 해당하는 값 가져오기

*/
package common.jdbc

import org.apache.spark.sql.functions.length

class JdbcGetPotentialPopData(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {
  import spark.implicits._

  val (url, connProp) = potentialPopJdbcProp

  def getPotentialPopData(srvcStrtDt: String, srvcEndDt: String) = {
    val potentialPopDataAll = spark.read.jdbc(url, "SM.PT_MSI", connProp)

    val potentialPopDataPre1 = potentialPopDataAll.
      select(
        'BS_YM.as("DATE"),
        'BJD_FUL_CD.as("SGG_CODE"),
        'PTNT_IDX
      )

    val potentialPopDataPre2 = potentialPopDataPre1.
      filter('DATE >= srvcStrtDt.toInt && 'DATE <= srvcEndDt.toInt).
      filter(length('SGG_CODE) === 5).
      na.fill(0, Seq("PTNT_IDX"))

    val potentialPopData = potentialPopDataPre2.distinct()

    potentialPopData
  }
}