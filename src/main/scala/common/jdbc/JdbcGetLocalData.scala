/*
localData get 기능 jdbc func

*/
package common.jdbc

class JdbcGetLocalData(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  val (url, connProp) = localDataJdbcProp()

  //  LOCAL_DATA COMMON_INFO 테이블 데이터 중 최신 데이터
  def getRcntCommonInfoAll = {
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val rcntOrder = Window.partitionBy('mgtNo, 'opnSfTeamCode, 'opnSvcId).orderBy('updateDt.desc, 'RGSTR_TIME.desc)

    val cmmnData = spark.read.jdbc(url, "CLCTDATA.LCL_CMN_INFO", connProp)
    val rankCmmnData = cmmnData.withColumn("rank", rank().over(rcntOrder))
    val rcntCmmnData = rankCmmnData.filter('rank === 1)

    val rcntCmmnInfoPre = rcntCmmnData.select(
      'mgtNo, //개방자치단체코드
      'opnSfTeamCode, //관리번호
      'opnSvcId, //개방서비스ID
      'opnSvcNm, //개방서비스명
      'siteWhlAddr, //지번주소
      'rdnWhlAddr, //도로명주소
      'apvPermYmd.cast("Int").cast("String"), //인허가일자
      'apvCancelYmd.cast("Int").cast("String"), //인허가취소일자
      'dcbYmd.cast("Int").cast("String"), //폐업일자
      'clgStdt.cast("Int").cast("String"), //휴업시작일자
      'clgEnddt.cast("Int").cast("String"), //휴업종료일자
      'ropnYmd.cast("Int").cast("String"), //재개업일자
      'trdStateNm, //영업상태명
      'uptaeNm //업태구분명
    )

    //  db에 날짜 데이터 소수점이 있는 String으로 들어있음 바꿔주고 정상적인 8자리 날짜 데이터만 yyyymm 형태로 다시 넣어주기
    val rcntCmmnInfo = rcntCmmnInfoPre.
      withColumn("apvPermYmd",
        when(length('apvPermYmd) === 8, substring('apvPermYmd, 0, 6)).otherwise(null)).
      withColumn("apvCancelYmd",
        when(length('apvCancelYmd) === 8, substring('apvCancelYmd, 0, 6)).otherwise(null)).
      withColumn("dcbYmd",
        when(length('dcbYmd) === 8, substring('dcbYmd, 0, 6)).otherwise(null)).
      withColumn("clgStdt",
        when(length('clgStdt) === 8, substring('clgStdt, 0, 6)).otherwise(null)).
      withColumn("clgEnddt",
        when(length('clgEnddt) === 8, substring('clgEnddt, 0, 6)).otherwise(null)).
      withColumn("ropnYmd",
        when(length('ropnYmd) === 8, substring('clgEnddt, 0, 6)).otherwise(null))

    rcntCmmnInfo
  }
}