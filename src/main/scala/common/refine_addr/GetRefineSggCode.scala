/*
get refine_addr sgg code

*/
package common.refine_addr

import org.jsoup.Jsoup

class GetRefineSggCode extends java.io.Serializable {

  //  다음주소 document 가져오기
  def getDoc(rowAddr: String) = {
    val regex1 = """\([^)]*\)""".r
    val regex2 = """\d층""".r
    val cleanAddr1 = regex1.replaceAllIn(rowAddr, "")
    val cleanAddr2 = regex2.replaceAllIn(cleanAddr1, "")
    val cleanAddr = cleanAddr2.trim
    val doc = Jsoup.connect("https://spi.maps.daum.net/postcode/search").
      data("cpage1", "1").
      data("origin", "https,//spi.maps.daum.net").
      data("isp", "N").
      data("isgr", "N").
      data("isgj", "N").
      data("plrgt", "1.5").
      data("us", "on").
      data("msi", "10").
      data("ahs", "off").
      data("whas", "500").
      data("zn", "Y").
      data("sm", "on").
      data("CWinWidth", "400").
      data("fullpath", "/postcode/guidessl").
      data("a51", "off").
      data("region_name", cleanAddr).
      data("cq", cleanAddr).
      get()

    doc
  }

  //  검색 결과를 사용할 수 있는지 체크
  def getCheckValue(doc: org.jsoup.nodes.Document) = {
    //  경고메세지 확인
    val alertDoc = doc.select(".emph_bold")
    val alert = if (!alertDoc.isEmpty) alertDoc.first().text() else ""

    //  검색결과 개수 확인
    //  페이지 수
    val numPageDoc = doc.select(".num_page")
    val numpage = if (!numPageDoc.isEmpty) numPageDoc.first().attr("data-pagetotal").toInt else 0
    //  첫번째 장에 나온 결과 수
    val numfirstPage = doc.select(".list_post li").size
    //  합
    val count = numpage * numfirstPage

    //  검색결과가 없거나 경고 메세지 존재하는 경우 사용 못함
    val check = if (count == 0 || alert == "검색결과가 없습니다." || alert == "검색결과가 많습니다.") false else true

    check
  }

  //  시군구 코드 추출
  def getSggCode(doc: org.jsoup.nodes.Document) = {
    val detailAddr = doc.select(".list_post>li").first()
    val sggCode = detailAddr.attr("data-sigungu_code")

    sggCode
  }

  def runGetRefineSggCode(rowAddr: String) = {
    val doc = getDoc(rowAddr)
    val check = getCheckValue(doc)
    val refineSggCode = if (check) {
      getSggCode(doc)
    }
    else null

    refineSggCode
  }
}