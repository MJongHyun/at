/*
수집한 데이터 서버 데이터 베이스에서 get 기능 jdbc func
*/
package common.jdbc

class JdbcGetCollectData(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  val (url, connProp) = martDataJdbcProp()

  //  요식업 업종 분류 데이터
  def getCateringCategoryData = {
    val cateringCategoryData = spark.read.jdbc(url, "AT.CATERING_CATEGORY", connProp)

    cateringCategoryData
  }

  //  시군구 면적 데이터
  def getSggArea = {
    val sggArea = spark.read.jdbc(url, "AT.SGG_AREA", connProp)

    sggArea
  }

  //  시도 시군구 코드 기준 데이터
  def getAreaCode = {
    val areaCode = spark.read.jdbc(url, "AT.AREA_CODE", connProp)

    areaCode
  }
}