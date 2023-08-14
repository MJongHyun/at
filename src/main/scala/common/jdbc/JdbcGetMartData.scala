/*
MartData get 기능 jdbc func

*/
package common.jdbc

class JdbcGetMartData(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

    import spark.implicits._

    val (url, connProp) = martDataJdbcProp()

    //  요식업 지방 인허가 마트 데이터
    def getCateringMartData(srvcStrtDt: String, srvcEndDt: String) = {
        val cateringMartDataPre1 = spark.read.jdbc(url, "AT.CATERING_MRT", connProp)
        //  필요한 데이터 기간 안의 데이터만 추출
        val cateringMartDataPre2 = cateringMartDataPre1.
          filter('DATE >= srvcStrtDt.toInt && 'DATE <= srvcEndDt.toInt)

        val cateringMartData = cateringMartDataPre2.drop("IDX")

        cateringMartData
    }

    //  지방인허가 요식업종 시군구별 업체수 마트 데이터
    def getCateringSggComCntData(srvcStrtDt: String, srvcEndDt: String) = {
        val cateringSggComCntDataPre = spark.read.jdbc(url, "AT.CATERING_SGG_COM_CNT", connProp)
        //  필요한 데이터 기간 안의 데이터만 추출
        val cateringSggComCntData = cateringSggComCntDataPre.
          filter('DATE >= srvcStrtDt.toInt && 'DATE <= srvcEndDt.toInt)

        cateringSggComCntData
    }

    //  주소, 정제해서 추출한 시군구 코드
    def getAddrSggCode = {
        val addrSggCode = spark.read.jdbc(url, "AT.ADDR_SGG", connProp)

        addrSggCode
    }

    //  거주인구 마트 데이터
    def getResidentPopMart(srvcStrtDt: String, srvcEndDt: String) = {
        val residentPopMartPre = spark.read.jdbc(url, "AT.RESIDENT_POP", connProp)
        //  필요한 데이터 기간 안의 데이터만 추출
        val residentPopMart = residentPopMartPre.
          filter('DATE >= srvcStrtDt.toInt && 'DATE <= srvcEndDt.toInt)

        residentPopMart
    }

    //  근로인구 마트 데이터
    def getPensionPopMart(srvcStrtDt: String, srvcEndDt: String) = {
        val pensionPopMartPre1 = spark.read.jdbc(url, "AT.PENSION_AMT_POP", connProp)
        //  필요한 데이터 기간 안의 데이터만 추출
        val pensionPopMartPre2 = pensionPopMartPre1.
          filter('DATE >= srvcStrtDt.toInt && 'DATE <= srvcEndDt.toInt)
        val pensionPopMart = pensionPopMartPre2.drop("salaryAverage")

        pensionPopMart
    }

    //  평균 급여 마트 데이터
    def getPensionAmtMart(srvcStrtDt: String, srvcEndDt: String) = {
        val pensionAmtMartPre1 = spark.read.jdbc(url, "AT.PENSION_AMT_POP", connProp)
        //  필요한 데이터 기간 안의 데이터만 추출
        val pensionAmtMartPre2 = pensionAmtMartPre1.
          filter('DATE >= srvcStrtDt.toInt && 'DATE <= srvcEndDt.toInt)
        val pensionAmtMart = pensionAmtMartPre2.drop("workingPop")

        pensionAmtMart
    }

    // 평준화 지수 기준 MAX 값
    def getStandardMaxIndex(scoreTableName: String) = {
        val standardMaxIndexPre1 = spark.read.jdbc(url, "AT.STDR_IDEX_CRTR", connProp)
        //  지수에 맞는 MAX 값 추출
        val standardMaxIndexPre2 = standardMaxIndexPre1.filter('TBL_NM === scoreTableName)
        val standardMaxIndex = standardMaxIndexPre2.select("MAX_IDX")

        standardMaxIndex
    }
}