/*
make catering mart data

*/
package common.mart_data

import common.jdbc.{JdbcGetCollectData, JdbcGetLocalData, JdbcGetMartData, JdbcSaveMartData}
import common.refine_addr.GetRefineSggCode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, explode, lit, regexp_replace, trim, udf, when}
import org.joda.time.format.DateTimeFormat
import org.joda.time.Months

import scala.util.{Failure, Success, Try}

class GetCateringMartData(spark: org.apache.spark.sql.SparkSession) extends java.io.Serializable {

  import spark.implicits._

  def getSourceData = {
    //  최신 로컬 데이터 전체
    val jdbcGetLocalDataObj = new JdbcGetLocalData(spark)
    val rcntCmmnInfoData = jdbcGetLocalDataObj.getRcntCommonInfoAll

    //  요식업종
    val jdbcGetCollectDataObj = new JdbcGetCollectData(spark)
    val cateringCategory = jdbcGetCollectDataObj.getCateringCategoryData

    (rcntCmmnInfoData, cateringCategory)
  }

  //  로컬데이터 중 요식업종만 추출
  def getRcntCateringInfoData(rcntCmmnInfoData: DataFrame, cateringCategory: DataFrame) = {
    //  지방인허가 데이터의 소분류 null 값을 '미분류'로 처리 (요식업종 데이터는 미분류로 되어있음)
    val rcntCmmnInfo = rcntCmmnInfoData.na.fill("미분류", Seq("uptaeNm"))
    val rcntCateringInfoData = cateringCategory.
      join(rcntCmmnInfo, Seq("opnSvcNm", "uptaeNm"))

    rcntCateringInfoData
  }

  //  영업기간을 계산하기 위해 영업시작일, 영업종료일, 휴업시작일, 휴업종료일을 세팅
  def setDateValue(rcntCateringInfoData: DataFrame, srvcStrtDt: String, srvcEndDt: String) = {
    //  영업시작일: 인허가일이 있고 인허가일이 서비스제공 데이터 시작일보다 이후 시점인 경우 해당 값을 쓰고
    //  그렇지 않은 경우는 서비스제공 데이터 시작일 (2016년 01월)로 가정, (세비스제공 데이터 시작일보다 전 데이터는 필요 없음)
    val cateringAddStartDate = rcntCateringInfoData.
      withColumn("startDate", when('apvPermYmd.isNotNull && 'apvPermYmd > srvcStrtDt, 'apvPermYmd).
        otherwise(srvcStrtDt))

    //  영업종료일: 휴업시작일자가 있고, 휴업종료일자(재개업일자, 휴업종료일자)가 없는 경우에는 휴업시작일자가 영업종료일.
    //  이 경우가 아니면,폐업 일자가 있는 경우 해당 값을 쓰고, 그렇지 않은 경우에는 인허가취소일자
    //  세 값이 모두 없는 경우에는 영업상태명이 '영업/정상'인 경우 영업중으로 가정, '영업/정상'이 아닌경우는 데이터 버림
    val cateringAddEndDatePre1 = cateringAddStartDate.
      withColumn("endDate",
        when('clgStdt.isNotNull && 'clgEnddt.isNull && 'ropnYmd.isNull, 'clgStdt).otherwise(
          when('dcbYmd.isNotNull, 'dcbYmd).otherwise(
            when('apvCancelYmd.isNotNull, 'apvCancelYmd).otherwise(
              when('trdStateNm === "영업/정상", srvcEndDt).otherwise(null)))))

    //  영업종료일 판단 아래 버리는 데이터 제거,
    //  서베스 제공 데이터 종료일보다 이후 데이터는 필요 없기 때문에 서비스 제공 데이터 종료일보다 이후 시점 데이터는 서비스 제공데이터 종료일을 넣어줌
    val cateringAddEndDatePre2 = cateringAddEndDatePre1.filter('endDate.isNotNull)
    val cateringAddEndDate = cateringAddEndDatePre2.
      withColumn("endDate", when('endDate > srvcEndDt, srvcEndDt).otherwise('endDate))

    //  휴업시작일: 휴업시작일자, 휴업종료일자(재개업일자 or 휴업종료일자)가 있고 휴업종료일자보다 작거나 같은 날짜일 경우로 한정
    val cateringAddStopStartDate = cateringAddEndDate.
      withColumn("stopStartDate",
        when('clgStdt.isNotNull &&
          (('clgEnddt.isNotNull && ('clgStdt <= 'clgEnddt)) ||
            ('ropnYmd.isNotNull && ('clgStdt <= 'ropnYmd))), 'clgStdt).otherwise(null))

    //  휴업종료일: 휴업시작일이 없는 경우 null, 재개업일자가 있는 경우 해당 값을 쓰고, 그렇지 않은 경우는 휴업종료일자, 휴업시작일보다 같거나 커야 사용
    val cateringAddStopEndDate = cateringAddStopStartDate.
      withColumn("stopEndDate", when('stopStartDate.isNull, null).otherwise(
        when('ropnYmd.isNotNull && ('clgStdt <= 'ropnYmd), 'ropnYmd).otherwise(
          when('clgEnddt.isNotNull && ('clgStdt <= 'clgEnddt), 'clgStdt).otherwise(null))))

    //  서비스 제공 기간 내의 데이터만 추출
    val cateringAddDate = cateringAddStopEndDate.
      filter('startDate >= srvcStrtDt && 'endDate <= srvcEndDt && 'startDate <= 'endDate).
      select(
        'mainCategory,
        'opnSvcNm,
        'uptaeNm,
        'mgtNo,
        'opnSfTeamCode,
        'opnSvcId,
        'siteWhlAddr,
        'rdnWhlAddr,
        'startDate,
        'endDate,
        'stopStartDate,
        'stopEndDate
      )

    cateringAddDate
  }

  def getCateringTimeSeriesData(cateringAddAreaCode: DataFrame) = {
    //  영업 기간 yyyymm 리스트 구하기
    val monthRangeUdf = udf((startDate: String, endDate: String, stopStartDate: String, stopEndDate: String) => {
      val formatter = DateTimeFormat.forPattern("yyyyMM")
      val monthList = if (startDate != null && endDate != null) {
        //  전처리에서 걸러지지 못한 날짜 형식에 맞지 않는 데이터가 있을 수 있기 때문에 오류 방지용 Try match 사용
        //  날짜 형식 맞지 않는 데이터는 사용할 수 없다 판단
        Try {
          val start = formatter.parseDateTime(startDate)
          val end = formatter.parseDateTime(endDate)
          val monthsBetween = Months.monthsBetween(start, end)
          val monthsRange = monthsBetween.
            toString.
            replace("P", "").
            replace("M", "").
            toInt

          //  휴업기간이 있는 데이터는 휴업기간을 영업기간에 포함시키지 않기 위함
          val stopMonthList = if (stopStartDate != null && stopEndDate != null) {
            val stopStart = formatter.parseDateTime(stopStartDate)
            val stopEnd = formatter.parseDateTime(stopEndDate)
            val stopMonthsBetween = Months.monthsBetween(stopStart, stopEnd)
            val stopMonthsRange = stopMonthsBetween.
              toString.
              replace("P", "").
              replace("M", "").
              toInt

            val stopMonthVec = for (stopMonth <- 0 to stopMonthsRange)
              yield stopStart.plusMonths(stopMonth).toString(formatter)

            stopMonthVec.toList
          } else List()

          val monthVec = for (month <- 0 to monthsRange
                              if !stopMonthList.contains(start.plusMonths(month).toString(formatter)))
          yield start.plusMonths(month).toString(formatter)

          monthVec.toList
        } match {
          case Success(result) => result
          case Failure(exception) => List()
        }
      } else List()

      monthList
    })

    val cateringAddMonthRange = cateringAddAreaCode.
      withColumn("monthRange", monthRangeUdf('startDate, 'endDate, 'stopStartDate, 'stopEndDate))

    //  영업기간 별 데이터로 explode
    val cateringTimeSeriesDataPre = cateringAddMonthRange.withColumn("DATE", explode('monthRange))

    val cateringTimeSeriesData = cateringTimeSeriesDataPre.drop(
      "startDate",
      "endDate",
      "stopStartDate",
      "stopEndDate",
      "monthRange"
    )

    cateringTimeSeriesData
  }

  def getTrimAddr(cateringAddDate: DataFrame) = {
    //  맨 앞 뒤 공백 제거
    val cateringAddrRtrim1 = cateringAddDate.
      withColumn("siteWhlAddr1", trim('siteWhlAddr)).
      withColumn("rdnWhlAddr1", trim('rdnWhlAddr))
    val cateringAddrRtrim2 = cateringAddrRtrim1.
      drop("siteWhlAddr", "rdnWhlAddr")
    val preCateringAddAreaCode = cateringAddrRtrim2.
      withColumnRenamed("siteWhlAddr1", "siteWhlAddr").
      withColumnRenamed("rdnWhlAddr1", "rdnWhlAddr")

    preCateringAddAreaCode
  }

  def getRefineAddr(preCateringAddAreaCode:DataFrame, addrType:String) = {
    //  시군구 코드 추출 udf
    val getRefineSggCodeObj = new GetRefineSggCode
    val getRefineSggCodeUdf = udf((addr: String) => {
      val sggCode = getRefineSggCodeObj.runGetRefineSggCode(addr)

      sggCode
    })

    //  기존 정제해서 시군구코드 있는 주소 가져오기
    val jdbcGetMartDataObj = new JdbcGetMartData(spark)
    val existCodeAddr = jdbcGetMartDataObj.getAddrSggCode
    val existAddr = existCodeAddr.select("addr").distinct()

    val rawAddr1 = preCateringAddAreaCode.select(addrType).distinct()
    val rawAddr = rawAddr1.withColumnRenamed(addrType, "addr")

    val noExistAddr1 = rawAddr.except(existAddr)
    val noExistAddr = noExistAddr1.filter('addr.isNotNull).distinct()

    val sggCodeAddr = noExistAddr.withColumn("SGG_CODE", getRefineSggCodeUdf('addr))

    //  새로 정제한 주소 DB insert
    val jdbcSaveMartDataObj = new JdbcSaveMartData
    jdbcSaveMartDataObj.saveAddrSggCodeData(sggCodeAddr)
  }

  //  주소 시군구 코드 추춯
  def getCateringAreaCode(preCateringAddAreaCode: DataFrame) = {
    //  기존 정제해서 시군구코드 있는 주소 가져오기
    val jdbcGetMartDataObj = new JdbcGetMartData(spark)
    val existCodeAddr = jdbcGetMartDataObj.getAddrSggCode

    // 새로 정제한 주소까지 모두 가져와서 추출한 시군구 코드 부착
    val rdnWhlAddrSggAllPre = preCateringAddAreaCode.
      join(existCodeAddr, 'rdnWhlAddr === 'addr, "leftouter")
    val rdnWhlAddrSggAll = rdnWhlAddrSggAllPre.
      withColumnRenamed("SGG_CODE", "sggCodeRdnWhl").
      drop("addr")
    val siteWhlAddrSggAllPre = rdnWhlAddrSggAll.
      join(existCodeAddr, 'siteWhlAddr === 'addr, "leftouter")
    val siteWhlAddrSggAll = siteWhlAddrSggAllPre.
      withColumnRenamed("SGG_CODE", "sggCodeSiteWhl").
      drop("addr")
    val cateringAreaCodePre = siteWhlAddrSggAll.
      withColumn("SGG_CODE", when('sggCodeRdnWhl.isNotNull, 'sggCodeRdnWhl).
        otherwise('sggCodeSiteWhl))
    val cateringAreaCode = cateringAreaCodePre.
      filter('SGG_CODE.isNotNull).
      drop("siteWhlAddr", "rdnWhlAddr", "sggCodeRdnWhl", "sggCodeSiteWhl")

    cateringAreaCode
  }

  //  업종 대,중,소별, 시군구별, 날짜별 업체수
  def getCateringSggComCntData(cateringAddAreaCode: DataFrame) = {
    val cateringSggComCntData = cateringAddAreaCode.
      groupBy('DATE, 'mainCategory, 'opnSvcNm, 'uptaeNm, 'SGG_CODE).
      agg(count("*").as("comCount"))

    cateringSggComCntData
  }

  //  지방인허가 데이터 마트 데이터 저장
  def saveCateringMartData(cateringAddAreaCode: DataFrame) = {
    val saveMartDataObj = new JdbcSaveMartData
    saveMartDataObj.saveCateringMartData(cateringAddAreaCode)
  }

  //  시군구별, 업종별, 업체수 데이터 저장
  def saveCateringSggComCntData(cateringSggComCntData: DataFrame) = {
    val saveMartDataObj = new JdbcSaveMartData
    saveMartDataObj.saveCateringSggComCntData(cateringSggComCntData)
  }

  def runGetCateringMartData(srvcStrtDt: String, srvcEndDt: String) = {
    //  최신로컬데이터 전체, 요식업 업종 데이터, 기준 지역 데이터
    val (rcntCmmnInfoData, cateringCategory) = getSourceData
    //  로컬데이터 전체에서 요식업만 추출
    val rcntCateringInfoData = getRcntCateringInfoData(rcntCmmnInfoData, cateringCategory)
    //  영업날짜 관련 변수들 세팅
    val cateringAddDate = setDateValue(rcntCateringInfoData, srvcStrtDt, srvcEndDt)
    //  주소 정제 전처리
    val preCateringAddAreaCode = getTrimAddr(cateringAddDate)
    //  도로명주소 정제
    getRefineAddr(preCateringAddAreaCode, "rdnWhlAddr")
    //  지번주소 정제
    getRefineAddr(preCateringAddAreaCode, "siteWhlAddr")
    //  주소 시군구 코드 추출
    val cateringAddAreaCode = getCateringAreaCode(preCateringAddAreaCode)
    //  영업월별 데이터 생성 (시계열 데이터 생성)
    val cateringTimeSeriesData = getCateringTimeSeriesData(cateringAddAreaCode)
    //  saveCateringMartData(cateringTimeSeriesData)

    //  지역별 월별 업종별 업체수
    val cateringSggComCntData = getCateringSggComCntData(cateringTimeSeriesData)
    saveCateringSggComCntData(cateringSggComCntData)
  }
}