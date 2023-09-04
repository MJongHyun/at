# AT 센터 지수 생성 코드

## 요약
    상권(시군구)별 지수 추출

## 실행 object
<파라미터: 원하는 분석 시작년월(yyyymm), 종료년월(yyyymm)>
1. (기본) 필요한 기간 안의 지수 추출 프로세스를 전부 실행시키고 싶은 경우(마트 포함): RunAllAt
2. 마트 데이터 제외 지수만 모두 실행시키고 싶은 경우: RunAllScore
3. 마트 데이터만 전부 실행시키고 싶은 경우: RunAllMart
4. 특정 마트 데이터 실행시키고 싶은 경우:
    + 지방인허가 요식업종 마트 데이터: RunCateringMart
    + 거주인구 마트 데이터: RunResidentPopulationMart
    + 근로인구, 급여 마트 데이터: RunSggPensionAmtPopMart
5. 특정 지수 실행시키고 싶은 경우:
    + 상권별 요식업종 밀집도 지수: RunCateringDensityScore
    + 상권별 요식업종 배후인구 지수: RunBackPopulationScore
    + 상권별 잠재구매력 지수: RunPotentialPurchasingPowerScore
    + 상권별 요식업종 균형 지수: RunCateringBalanceScore
    + 상권별 요식업종 변화율 지수: RunCateringChangeScore
    + 상권별 요식업종 활성도 지수: RunCateringActivityScore
## jar파일 및 실행
* sshpass -pscp@1234 ssh -o StrictHostKeyChecking=no hdfsuser@0.0.0.0
* jar 파일: 0.0.0.0 서버 /home/hdfsuser/at/at.jar
* log 파일: 0.0.0.0 서버 /home/hdfsuser/at/at_log/일별로그
* 현재 배치 작업 없음
* 실행 방법
    1. /home/hdfsuser/batch/at/at.sh 쉘 스크립트
        * 한달 전 데이터 기본 실행
        * 돌리고 싶은 특정 달 있으면 DATE 변수에 yyyymm 형식으로 값 입력
    2. spark-submit으로 실행하고 싶은 오브젝트 실행
        ``` 
        예시
        spark-submit \
            --driver-memory 8g \
            --executor-memory 10g \
            --num-executors 5 \
            --conf spark.driver.maxResultSize=4g \
            --conf spark.debug.maxToStringFields=100 \
            --driver-java-options "-Dlog4j.configuration=log4j.properties" \
            --driver-class-path /home/hdfsuser/at/at.jar \
            --class make_score.all_score.RunAllScore\
            /home/hdfsuser/at/at.jar 202109 202109
        ```
## 2022.06.08 작성 기준 상황
* 마트 데이터 저장위치: 0.0.0.0 서버의 AT DB에 저장됨
## 프로세스 순서
1. 시작년월, 종료년월 인자로 받기
2. 마트 데이터 생성
    1. 지방인허가 요식업종 마트 데이터 생성
        1. 최신 로컬 데이터 전체 가져오기
            * 127.0.0.1 서버의 LOCAL_DATA DB의 COMMON_INFO 테이블 데이터 중 최신 데이터 가져옴
            * 코드 개발 당시 중복값 이슈, 자료형 오류 등이 있었음. 다 처리해서 정상적인 데이터 가져옴
        2. 요식업종 데이터 가져오기
            * 0.0.0.0 서버의 AT DB의 CATERING_CATEGORY 테이블 데이터 가져옴
        3. 로컬 데이터 중 요식업종만 추출
        4. 영업 기간을 계산하기 위해 영업시작일, 영업종료일, 휴업시작일, 휴업종료일 세팅
            * 로컬데이터의 특성을 파악해서 개발자가 판단 / 정의하여 진행하였음.
                ```
                * 판단 가능한 정보:
                    - 영업시작일: 인허가일자, 휴업종료일자, 재개업일자
                    _ 영업종료일: 인허가취소일자, 폐업일자, 휴업시작일자
                    - 영업 기간 계산: 
                        - 인허가 시작일부터 인허가취소 혹은 폐업일까지
                        - 휴업시작일자와 휴업종료일자/재개업일자 있는 경우 해당 기간 제외
                    -  영업 상태 여부: 영업상태명

                * 예외처리
                    - 영업시작일 정보 없는 경우: 영업시작일을 데이터 제공 시작 년월인 201601로 가정
                    - 인허가취소일자/폐업일자 정보 없는 경우:
                        - 영업상태명이 ‘영업/정상’인 경우: 현재까지 영업중인 업소로 최신 데이터까지 영업기간
                        - 다른 경우 중 휴업시작일이 존재하는 경우: 인허가일자 ~ 휴업시작일자가 영업기간
                        - 나머지: 영업 기간을 알 수 없어 데이터 사용하지 않음
                    - 휴업시작일은 존재, 휴업종료일/재개업일자 없는 경우: 휴업시작일을 영업종료일로 취급

                * 영업 기간 식
                    - 영업시작일: 인허가일이 있는 경우 해당 값을 쓰고 그렇지 않은 경우는 2016년 01월(서비스 제공 시작 시점)로 가정,
                    - 영업종료일:
                        - 휴업종료일자(재개업일자, 휴업종료일자)가 없는 경우에는 휴업시작일자가 영업종료일.
                        - 이 경우가 아니면,폐업 일자가 있는 경우 해당 값을 쓰고, 그렇지 않은 경우에는 인허가취소일자.
                        - 세 값이 모두 없는 경우에는 영업상태명이 '영업/정상'인 경우 영업중으로 가정, '영업/정상'이 아닌경우는 데이터 버림
                    - 휴업시작일: 휴업시작일자, 휴업종료일자(재개업일자 or 휴업종료일자)가 있고 휴업종료일자보다 작거나 같은 날짜일 경우로 한정
                    - 휴업종료일: 휴업시작일이 없는 경우 null, 재개업일자가 있는 경우 해당 값을 쓰고, 그렇지 않은 경우는 휴업종료일자, 휴업시작일보다 같거나 커야 사용
                    - 영업기간: 영업시작일 ~ 영업종료일 - (휴업시작일 ~ 휴업종료일).
                ```
        5. 세팅한 값으로 서비스 제공 기간 내의 데이터만 추출
        6. 위에서 세팅한 값으로 영업기간 내의 yyyymm 데이터로 explode
            * 영업기간: 영업시작일 ~ 영업종료일 - (휴업시작일 ~ 휴업종료일) 에 맞는 년월 리스트 구하기
            * 영업년월 별 데이터로 explode
        7. 주소 정제
            1. 주소 정제 전처리
            2. 도로명 주소 정제 / 지번 주소 정제
                * 이미 정제되어 시군구 코드 있는 주소 값 가져오기 (0.0.0.0 서버의 AT DB의 ADDR_SGG)
                * 이미 정제된 주소가 아닌 주소 정제
                * DB에 저장
            3. 시군구 코드 부착
        8. 시군구별, 업종 대,중,소별, 업체수 데이터 생성 및 저장
    2. 거주 인구 마트 데이터 추출
        1. 127.0.0.1 서버의 PUBLIC_DATA DB의 POP_MIS 테이블 읽어오기
        2. 미추홀구 등 예외 처리
        3. 월별 시군구별 거주 인구 추출 및 저장
    3. 국민연금 마트 데이터 추출
        1. 127.0.0.1 서버의 PUBLIC_DATA DB의 PENSION 테이블 읽어오기
        2. 가입상태가 탈퇴(2)인 기업 제외하고 등록(1)인 상태의 데이터만 추출
        3. 직원수 0인 데이터 제외
        4. 시군구 코드 예외 처리
        5. 국민연금 납부액으로 업체 평균 급여 금액 데이터 추출 (계산식: AMT/MEMBER_CNT*100/9)
        6. 시군구별 근로자수, 평균 급여 추출 및 저장
3. 지수 추출 
    * 각 지수 추출이 끝났을 때 0.0.0.0 서버의 aatbc_db DB에 맞는 테이블에 저장됨.  
    1. 배후인구 지수 / 배후인구 평준화 지수 추출
    2. 요식업 활성도 지수 / 요식업 활성도 평준화 지수 추출
    3. 요식업 변화율 지수 / 요식업 변화율 평준화 지수 추출
    4. 요식업 밀집도 지수 / 요식업 밀집도 평준화 지수 추출
    5. 잠재구매력 지수 / 잠재구매력 평준화 지수 추출
    6. 분기가 끝나는 월인지 확인 후, 분기 종료 월이면 요식업 균형 지수 / 요식업 균형 평준화 지수 추출

## 코드 설명
* common: 공통 사용
    * jdbc: DB관련
        * package (jdbc package object): DB 접속 정보 및 커넥션 기능, db 접속 정보가 바뀌면 이 파일에서 수정하면 됨
            * localDataJdbcProp() -> (url, connProp)
                - 127.0.0.1 서버 LOCAL_DATA DB
            * publicDataJdbcProp() -> (url, connProp)
                - 127.0.0.1 서버 PUBLIC_DATA DB
            * martDataJdbcProp() -> (url, connProp)
                - 마트 데이터 저장용
                - 127.0.0.1 서버 AT DB
            * potentialPopJdbcProp() -> (url, connProp)
                - 인구 지수 중 잠재지수 값
                - 127.0.0.1 서버 SM DB
            * scoreJdbcPop() -> (url, connProp)
                - 지수 결과 저장
                - 0.0.0.0 서버 aatbc_db DB
        * JdbcGetCollectData(spark): 새로 수집하지 않는, 변하지 않는 값으로 쓰는 데이터들을 71번 서버 AT DB에서 가져옴
            * getCateringCategoryData -> 요식업 업종 분류 데이터(sql.DataFrame)
                - 요식업 업종 분류 데이터를 CATERING_CATEGORY 테이블에서 가져오는 함수
            * getSggArea -> 시군구 면적 데이터(sql.DataFrame)
                - 시군구 면적 데이터를 SGG_AREA 테이블에서 가져오는 함수
            * getAreaCode -> 시도 시군구 코드 기준 데이터(sql.DataFrame)
                - 시도 시군구 코드 기준 데이터를 AREA_CODE 테이블에서 가져오는 함수
        * JdbcGetLocalData(spark): localData 를 가져옴
            * getRcntCommonInfoAll -> localData(sql.DataFrame)
                1. COMMON_INFO 테이블을 읽어서 mgtNo, opnSfTeamCode, opnSvcId 컬럼 기준으로 updateDt와 RGSTR_TIME 컬럼 순서로 최신 데이터 한개만 가져옴
                    (코드 개발 당시 재수집 등의 이유로 중복 데이터가 존재했음. 중복 데이터 중 최신 값을 가져오기 위함)
                2. 필요한 컬럼만 select (컬럼은 코드 참고)
                3. 자료형 맞게 바꿔주고 날짜 데이터 소수점이 있는 String으로 들어있음 바꿔주고 정상적인 8자리 날짜 데이터만 yyyymm 형태로 다시 넣어주기
                    (코드 개발 당시 db 자료형이 잘못되어있었음. 자세한 내용은 코드 참고)
        * JdbcGetMartData(spark): 71번 AT DB에서 mart 데이터를 읽어오는 기능
            * getCateringMartData(데이터 시작yyyymm, 데이터 종료yyyymm) -> 요식업 지방 인허가 마트 데이터(sql.DataFrame)
                1. 요식업 지방 인허가 마트 데이터를 CATERING_MRT 테이블에서 읽어옴
                2. 인자로 받은 데이터 시작날짜와 종료날짜 사이에 있는 데이터만 가져옴
                3. 필요없는 컬럼인 IDX drop
            * getCateringSggComCntData(데이터 시작yyyymm, 데이터 종료yyyymm) -> 지방인허가 요식업종 시군구별 업체수 마트 데이터(Dataset[Row])
                1. 지방인허가 요식업종 시군구별 업체수 마트 데이터를 CATERING_SGG_COM_CNT 테이블에서 읽어옴
                2. 인자로 받은 데이터 시작날짜와 종료날짜 사이에 있는 데이터만 가져옴
            * getAddrSggCode -> 주소, 시군구 코드(sql.DataFrame)
                1. 주소, 시군구 코드를 ADDR_SGG 테이블에서 가져옴
            * getResidentPopMart(데이터 시작yyyymm, 데이터 종료yyyymm) -> 거주인구 마트 데이터(Dataset[Row])
                1. 거주인구 마트 데이터를 RESIDENT_POP 테이블에서 가져옴
                2. 인자로 받은 데이터 시작날짜와 종료날짜 사이에 있는 데이터만 가져옴
            * getPensionPopMart(데이터 시작yyyymm, 데이터 종료yyyymm) -> 근로인구 마트 데이터(sql.DataFrame)
                1. 평균 급여,근로인구 마트 데이터를 PENSION_AMT_POP 테이블에서 가져옴
                2. 인자로 받은 데이터 시작날짜와 종료날짜 사이에 있는 데이터만 가져옴
                3. 필요없는 컬럼인 salaryAverage(평균 급여) drop
            * getPensionAmtMart(데이터 시작yyyymm, 데이터 종료yyyymm) -> 평균 급여 마트 데이터(sql.DataFrame)
                1. 평균 급여,근로인구 마트 데이터를 PENSION_AMT_POP 테이블에서 가져옴
                2. 인자로 받은 데이터 시작날짜와 종료날짜 사이에 있는 데이터만 가져옴
                3. 필요없는 컬럼인 workingPop(근로인구) drop
            * getStandardMaxIndex(지수테이블이름) -> 평준화 지수 기준 MAX 값(sql.DataFrame)
                1. 평준화 지수 기준 MAX 값 데이터를 STDR_IDEX_CRTR 테이블에서 가져옴
                2. 인자로 받은 scoreTableName(지수테이블이름)과 TBL_NM 컬럼 값이 같은 것만 추출해 지수에 맞는 MAX 값 추출
                3. 필요한 값인 MAX_IDX 컬럼만 select
        * JdbcGetPotentialPopData(spark): 인구 지수 중 잠재지수 값 있는 98번 SM DB에서 잠재인구 지수 가져옴
            * getPotentialPopData(데이터 시작yyyymm, 데이터 종료yyyymm) -> 잠재인구지수 데이터(sql.DataFrame)
                1. PT_MSI 테이블 읽기
                2. 필요한 컬럼만 select, 컬럼 이름 변경
                3. 인자로 받은 데이터 시작날짜와 종료날짜 사이에 있는 데이터만 가져옴
                4. 법정동코드 자리가 5개인 것(시군구 레벨인 값만 가져오기 위함)만 추출
                5. 지수값이 null인 데이터 0으로 채우기
                6. distinct
        * JdbcGetPublicData(spark): 93번 서버의 PUBLIC_DATA DB 데이터
            * getSggMonthSeriesResidentPopulation(데이터 시작yyyymm, 데이터 종료yyyymm) -> 월별 시군구별 거주 인구 데이터(sql.DataFrame)
                1. POP_MIS 테이블 읽기
                2. 행정동 단위 데이터 필요없어서 행정동 이름 값이 null인 데이터만 추출
                3. 인자로 받은 데이터 시작날짜와 종료날짜 사이에 있는 데이터만 가져옴
                4. 필요한 컬럼만 추출
                5. 행정동 코드에서 시군구코드 추출
                6. 시군구 코드 예외 처리
            * getMonthSeriesPensionAmtMemberCnt(데이터 시작yyyymm, 데이터 종료yyyymm) -> 월별 업체 국민연금 납부액, 근로자수 데이터(sql.DataFrame)
                1. PENSION 테이블(국민연금 데이터) 읽기
                2. 가입상태가 탈퇴(2)인 기업 제외하고 등록(1)인 상태의 데이터 추출
                3. 직원수 0인 데이터 제외
                4. 인자로 받은 데이터 시작날짜와 종료날짜 사이에 있는 데이터 추출
                5. 필요한 컬럼만 추출
                6. 시도코드와 시군구(3자리)코드 합쳐서 시군구코드 생성
                7. 시군구코드 예외처리
        * JdbcSaveMartData: AT DB에 mart 데이터 저장하는 기능
            * saveCateringMartData(지방인허가 요식업 마트 데이터(DataFrame)): CATERING_MRT 테이블에 저장
            * saveCateringSggComCntData(시군구별, 업종별, 업체수 지방인허가 요식업 마트 데이터(DataFrame)): CATERING_SGG_COM_CNT 테이블에 저장
            * saveSggPensionAmtPopMartData(시군구별 평균 급여, 근로인구 마트 데이터(DataFrame)): PENSION_AMT_POP 테이블에 저장
            * saveResidentPopulationMartData(시군구별 거주 인구 수 데이터(DataFrame)): RESIDENT_POP 테이블에 저장
            * saveAddrSggCodeData(주소 정제 시군구 코드 데이터(DataFrame)): ADDR_SGG 테이블에 저장
        * JdbcSaveResultData: 68번 aatbc_db DB에 지수 결과 데이터 저장하는 기능
            * saveCateringDensityScore(요식업 밀집도 지수(DataFrame)): TB_TS_RBDI 테이블에 저장
            * saveBackPopulationScore(배후인구 지수(DataFrame)): TB_TS_RBBPI 테이블에 저장
            * savePotentialPurchasingPowerScore(잠재구매력 지수(DataFrame)): TB_TS_PPI 테이블에 저장
            * saveCateringActivityScore(활성도 지수(DataFrame)): TB_TS_RBAI 테이블에 저장
            * saveCateringBalanceScore(균형 지수(DataFrame)): TB_TS_RBBI 테이블에 저장
            * saveCateringChangeScore(변화율 지수(DataFrame)): TB_TS_RBCRI 테이블에 저장
    * mart_data: 공통 사용 데이터들을 각 지수 추출 코드에서 사용할 수 있는 형태, 마트 데이터로 만들고 DB 저장
        * GetCateringMartData(spark): 요식업 마트 데이터
            * getSourceData -> (최신 로컬 데이터 전체(DataFrame), 요식업종 데이터(DataFrame)): 소스 데이터 가져오기
            * getRcntCateringInfoData(최신 로컬 데이터 전체(DataFrame), 요식업종 데이터(DataFrame)) -> 로컬 데이터 중 요식업종인 데이터(DataFrame)
                1. 로컬 데이터의 소분류 null 값을 '미분류'로 변환 (요식업종 데이터에는 미분류라고 되어있어서 데이터 통일)
                2. 최신 로컬 데이터와 요식업종 데이터를 opnSvcNm, uptaeNm컬럼 기준으로 join
            * setDateValue(로컬 데이터 중 요식업종인 데이터(DataFrame), 데이터 시작yyyymm, 데이터 종료yyyymm) -> 데이터 기간 내의 요식업종 데이터
                : 프로세스 설명의 영업기간 계산 내용, 코드 참고
            * getCateringTimeSeriesData(데이터 기간 내의 요식업종 데이터(DataFrame)) -> 월별 시리즈 요식업종 데이터(DataFrame)
                1. 영업기간 내의 년월 리스트 구하기 (프로세스 설명의 영업기간 계산 내용, 코드 참고)
                2. 월별 데이터로 explode
                3. 필요한 컬럼만 select
            * getTrimAddr(월별 요식업종 데이터(DataFrame)) -> 주소 데이터 앞 뒤 공백 제거한 월별 요식업종 데이터(DataFrame)
            * getRefineAddr(주소 앞뒤 공백 제거한 월별 요식업종 데이터(DataFrame), 주소타입(String(rdnWhlAddr: 도로명, siteWhlAddr: 지번)))
                1. 기존 주소, 시군구코드 마트 가져오기
                2. 주소 마트에 있는 원천 주소 distinct
                3. 주소 앞뒤 공백 제거한 월별 요식업종 데이터에서 주소타입에 맞는 컬럼의 주소 값 distinct
                4. 3번 주소 중 2번 주소에 없는 주소 (3번주소 - 2번주소) distinct
                5. 주소 값 null인 주소 제거
                6. 주소 정제 프로세스 통해 시군구 코드 추출
                7. 새로 정제한 원천 주소, 시군구 코드 주소 마트에 저장
            * getCateringAreaCode(주소 앞뒤 공백 제거한 월별 요식업종 데이터(DataFrame)) -> 시군구 코드 추가한 월별 요식업종 데이터(DataFrame)
                1. 주소 마트 읽어오기
                2. 도로명주소 기준으로 주소 마트의 시군구 코드 값 부착
                3. 지번주소 기준으로 주소 마트의 시군구 코드 값 부착
                4. 도로명주소의 시군구코드, 지번주소의 시군구코드 순서의 우선순위로 시군구 코드값 뽑기
                5. 시군구 코드가 null인 데이터 (주소 정제가 안되는 데이터) 제거
                6. 필요없는 컬럼 drop
            * getCateringSggComCntData(시군구 코드 추가한 월별 요식업종 데이터(DataFrame)) -> 업종 대,중,소별 시군구별 월별 업체수 데이터(DataFrame)
                : 업종 대,중,소별 시군구별 월별로 데이터 수 count
            * saveCateringMartData(업종 대,중,소별 시군구별 월별 업체수 데이터(DataFrame))
                : 업종 대,중,소별 시군구별 월별 업체수 데이터 DB에 저장
            * runGetCateringMartData(데이터 시작yyyymm, 데이터 종료yyyymm): 요식업 마트 데이터 만드는 메인
                1. getSourceData 이용해서 데이터 소스 가져오기
                2. getRcntCateringInfoData 이용해서 로컬데이터 전체에서 요식업만 추출
                3. setDateValue 이용해서 영업날짜 관련 변수들 세팅하고 영업기간 내의 데이터만 추출
                4. getTrimAddr 이용해서 주소 정제 전처리
                5. getRefineAddr 이용해서 도로명 주소 정제
                6. getRefineAddr 이용해서 지번 주소 정제
                7. getCateringAreaCode 이용해서 시군구코드 부착
                8. getCateringTimeSeriesData 이용해서 시계열 데이터 생성
                9. getCateringSggComCntData 이용해서 지역별 월별 업종별 업체수 마트 생성
                10. saveCateringSggComCntData 이용해서 마트 DB에 저장
        * GetResidentPopulationMartData(spark): 거주인구 마트 데이터
            * getSourceData(데이터 시작yyyymm, 데이터 종료yyyymm) -> 거주 인구 데이터(DataFrame)
                : 데이터 기간 내의 거주 인구 가져오기
            * saveResidentPopulationMartData(거주인구데이터(DataFrame))
                : 거주 인구 데이터 DB에 저장
            * getResidentPopulationMartData(데이터 시작yyyymm, 데이터 종료yyyymm): 거주인구 마트 데이터 만드는 메인
                1. getSourceData 이용해서 거주 인구 데이터 가져오기
                2. saveResidentPopulationMartData 이용해서 마트 DB에 저장
        * GetSggPensionAmtPopMartData(spark): 근로자수, 평균급여 마트
            * getSourceData(데이터 시작yyyymm, 데이터 종료yyyymm) -> 국민 연금 데이터(DataFrame)
                : 데이터 기간 내의 국민 연금 데이터 가져오기
            * getSalaryAverageData(국민연금데이터(DataFrame)) -> 업체 평균 급여 금액 데이터(DataFrame)
                : AMT/MEMBER_CNT*100/9 식으로 업체 평균 급여 금액 추출
            * getSggPensionAmtPop(업체 평균 급여 금액 데이터(DataFrame)) -> 시군구별 평균 급여 금액, 근로자수 데이터(DataFrame)
                : 시군구 코드, 월별 날짜 기준으로 업체 평균 급여의 평균, 근로자수의 합 구하기
            * saveSggPensionAmtPopMartData(근로자수, 평균급여 마트 데이터(DataFrame))
                : 근로자수, 평균급여 마트 데이터 DB에 저장
            * getSggPensionAmtPopMartData(데이터 시작yyyymm, 데이터 종료yyyymm): 근로자수, 평균급여 마트 만드는 메인
                1. getSourceData 이용해서 국민연금 데이터 가져오기
                2. getSalaryAverageData 이용해서 업체 평균 급여 금액 데이터 생성
                3. getSggPensionAmtPop 이용해서 시군구별 평균 급여 금액, 근로자수 데이터 생성
                4. saveSggPensionAmtPopMartData 이용해서 마트 DB 저장
    * refine_addr: 주소 정제헤서 시군구 코드 구하는 기능
        * GetRefineSggCode
            * getDoc(원천주소(String)) -> 다음 주소 document
                1. 정규표현식 이용해서 ()와 그 안에 있는 글자 제거
                2. 정규표현식 이용해서 숫자+층 글자 제거
                (여러 시행착오를 통해 주소 정제할 때 위 두가지를 제거해서 검색하는 것이 정확도가 가장 높았음)
                3. Jsoup이용해서 다음 주소 document 가져오기 (parameter는 코드 참고)
            * getCheckValue(다음주소document(org.jsoup.nodes.Document)) -> 검색 결과 사용 가능 여부(Boolean)
                1. 경고 메세지 읽어오기
                2. 검색결과 페이지수 구하기
                3. 첫번째 장에 나온 결과수 구하기
                4. 검색결과 페이지 수와 첫번째 장에 나온 결과수 곱해서 검색 결과 개수를 대략 구함
                (사실 현재 버전엔 검색 결과 개수로 0인지 아닌지만 확인하기 때문에 페이지수 곱한 값 안구해도 됨. 원래 버전엔 검색 결과가 몇개 이상이면 믿고 안믿고를 따졌었음)
                5. 검색결과가 없거나 경고 메세지가 존재하는 경우엔 주소 정제 결과를 사용할 수 없다고 판단하여 False를 리턴하고 아닌경우에는 True 리턴
            * getSggCode(다음주소document(org.jsoup.nodes.Document)) -> 시군구 코드(String)
                : 다음 주소 document에서 시군구 코드 추출
            * runGetRefineSggCode(원천주소(String)) -> 시군구 코드(String): 주소 정제 메인
                1. getDoc 이용해서 다음 주소 document 가져오기
                2. getCheckValue 이용해서 검색 결과 사용 가능 여부 따지기
                3. 2번이 True 일 경우에만 getSggCode 이용해서 시군구 코드 추출 및 return False 면 null return
    * set_result_format: 결과 데이터 포맷 맞추는 기능
        * GetDateFormat(spark): 분석날짜에 해당하는 모든 년월 추출하는 기능
            * getMonthList(데이터 시작yyyymm, 데이터 종료yyyymm) -> 분석년월리스트(List[String])
                1. 데이터 시작일부터 데이터 종료일까지의 개월수를 구함
                2. 데이터 시작일부터 개월수만큼 한달씩 더하면서 분석날짜에 해당하는 모든 년월 yyyymm 리스트를 구함
        * GetDateQuarterFormat(spark): 분석날짜에 해당하는 모든 년월과 해당 분기를 추출하는 기능
            * getMonthQuarterList(데이터 시작yyyymm, 데이터 종료yyyymm) -> 분석년,년월,분기리스트(List[(Int, Int, Int)])
                1. 데이터 시작일부터 데이터 종료일까지의 개월수를 구함
                2. 데이터 시작일부터 개월수만큼 한달씩 더하면서 분석날짜에 해당하는 년, 년월, 분기 리스트를 구함
            * getMonthQuarterDataFrame(분석년,년월,분기리스트(List[(Int, Int, Int)])) -> 분석년,년월,분기 데이터(sql.DataFrame)
                : 리스트를 데이터프레임으로 변환
            * getDateQuarterFormat(데이터 시작yyyymm, 데이터 종료yyyymm) -> 분석년,년월,분기 데이터(sql.DataFrame): 분석날짜에 해당하는 모든 년월과 해당 분기 추출 메인
                1. getMonthQuarterList를 이용해서 분석년,년월,분기리스트를 구함
                2. getMonthQuarterDataFrame을 이용해서 분석년,년월,분기 데이터를 구함
        * SetCateringDateQuarterSggFormat(spark): 분석날짜, 요식업종, 분기, 시군구코드 별로 모든 데이터가 있도록 기준 틀 맞추고 해당 지수 값 없으면 0으로 넣어주는 기능
            * getSourceData(데이터 시작yyyymm, 데이터 종료yyyymm) -> (시군구코드 데이터(DataFrame), 요식업종 데이터(DataFrame), 분기 데이터(DataFrame)): 결과 데이터의 기준이 되는 데이터 소스들 가져오기
                1. 시군구 코드 데이터 가져오기
                2. 요식업종 데이터 가져오기
                3. 분기 데이터 가져오기
            * getAllCateringDateQuarterSggDf(시군구코드 데이터(DataFrame), 요식업종 데이터(DataFrame), 분기 데이터(DataFrame)) -> 결과에 포함되어야 하는 시군구, 요식업종, 분기, 날짜가 모두 있는 데이터(DataFrame) : 시군구 코드, 요식업종, 분기 데이터 크로스조인
            * getCateringDateQuarterSggFormat(결과 기준 데이터(DataFrame), 지수결과 데이터(DataFrame)) -> 결과 기준 맞춰진 지수 결과 데이터(DataFrame)
                1. 결과 기준 데이터기준으로 지수결과 데이터 붙이기
                2. 지수값 없어서 null인 데이터 0으로 바꿔주기
            * setCateringDateQuarterSggFormat(데이터 시작yyyymm, 데이터 종료yyyymm, 지수결과 데이터(DataFrame)) -> 결과 기준 맞춰진 지수 결과 데이터(DataFrame): 결과 기준 맞춰진 지수 결과 데이터 만들기 메인
                1. getSourceData을 이용해서 결과에 다 있어야하는 기준인 데이터들을 가져오기
                2. getAllCateringDateQuarterSggDf을 이용해서 1번에서 가져온 데이터들을 합쳐서 결과 기준 틀 만들기
                3. getCateringDateQuarterSggFormat을 이용해서 기준 데이터에 지수결과 데이터를 붙이고, 지수결과에 없어서 null이 된 지수는 0으로 채우기
        * SetCateringDateSggFormat(spark): 분석날짜, 요식업종, 시군구코드 별로 모든 데이터가 있도록 기준 틀 맞추고 해당 지수 값 없으면 0으로 넣어주는 기능
            * getSourceData(데이터 시작yyyymm, 데이터 종료yyyymm) -> (시군구코드 데이터(DataFrame), 요식업종 데이터(DataFrame), 날짜 데이터(DataFrame)): 결과 데이터의 기준이 되는 데이터 소스들 가져오기
                1. 시군구 코드 데이터 가져오기
                2. 요식업종 데이터 가져오기
                3. 날짜 데이터 가져오기
            * getAllCateringDateSggDf(시군구코드 데이터(DataFrame), 요식업종 데이터(DataFrame), 날짜 데이터(DataFrame)) -> 결과에 포함되어야 하는 시군구, 요식업종, 날짜가 모두 있는 데이터(DataFrame)
                1. 시군구 코드, 요식업종, 날짜 데이터 크로스조인
                2. 컬렴명 변경
            * getCateringDateSggFormat(결과 기준 데이터(DataFrame), 지수결과 데이터(DataFrame)) -> 결과 기준 맞춰진 지수 결과 데이터(DataFrame)
                1. 결과 기준 데이터기준으로 지수결과 데이터 붙이기
                2. 지수값 없어서 null인 데이터 0으로 바꿔주기
            * setCateringDateSggFormat(데이터 시작yyyymm, 데이터 종료yyyymm, 지수결과 데이터(DataFrame)) -> 결과 기준 맞춰진 지수 결과 데이터(DataFrame): 결과 기준 맞춰진 지수 결과 데이터 만들기 메인
                1. getSourceData을 이용해서 결과에 다 있어야하는 기준인 데이터들을 가져오기
                2. getAllCateringDateSggDf 이용해서 1번에서 가져온 데이터들을 합쳐서 결과 기준 틀 만들기
                3. getCateringDateSggFormat 이용해서 기준 데이터에 지수결과 데이터를 붙이고, 지수결과에 없어서 null이 된 지수는 0으로 채우기
        * SetDateSggFormat(spark): 분석날짜, 시군구코드 별로 모든 데이터가 있도록 기준 틀 맞추고 해당 지수 값 없으면 0으로 넣어주는 기능
            * getSourceData(데이터 시작yyyymm, 데이터 종료yyyymm) -> (시군구코드 데이터(DataFrame), 날짜 데이터(DataFrame)): 결과 데이터의 기준이 되는 데이터 소스들 가져오기
                1. 시군구 코드 데이터 가져오기
                3. 날짜 데이터 가져오기
            * getAllDateSggDf(시군구코드 데이터(DataFrame), 날짜 데이터(DataFrame)) ->  -> 결과에 포함되어야 하는 시군구, 날짜가 모두 있는 데이터(DataFrame)
                1. 시군구 코드, 날짜 데이터 크로스조인
                2. 컬렴명 변경
            * getDateSggFormat(결과 기준 데이터(DataFrame), 지수결과 데이터(DataFrame)) -> 결과 기준 맞춰진 지수 결과 데이터(DataFrame)
                1. 결과 기준 데이터기준으로 지수결과 데이터 붙이기
                2. 지수값 없어서 null인 데이터 0으로 바꿔주기
            * setDateSggFormat(데이터 시작yyyymm, 데이터 종료yyyymm, 지수결과 데이터(DataFrame)) -> 결과 기준 맞춰진 지수 결과 데이터(DataFrame): 결과 기준 맞춰진 지수 결과 데이터 만들기 메인
                1. getSourceData을 이용해서 결과에 다 있어야하는 기준인 데이터들을 가져오기
                2. getAllDateSggDf 이용해서 1번에서 가져온 데이터들을 합쳐서 결과 기준 틀 만들기
                3. getDateSggFormat 이용해서 기준 데이터에 지수결과 데이터를 붙이고, 지수결과에 없어서 null이 된 지수는 0으로 채우기
    * setting_function
        * CheckQuarterEndMonth: 분기별 데이터인 균형지수에서 사용하기 위한 분기 관련 기능
            * checkQuarterEndMonth(데이터 종료yyyymm) -> 분기 끝나는 월이 맞는지 여부(Boolean)
                1. 데이터종료날짜 yyyyMM 포맷 데이트타임 타입으로 변환
                2. 몇월인지 구한 후, 3으로 나눈 나머지로 분기 끝나는 월인지 체크
            * getQuarterStartMonth(데이터 종료yyyymm) ->  분기에 해당하는 시작 년월(-2월) yyyymm(String)
                1. 데이터종료날짜 yyyyMM 포맷 데이트타임 타입으로 변환
                2. 두달 전 날짜 추출 및 yyyyMM String으로 변환
        * CmnFunc: 공통 사용 기능 (spark, logger)
            * getSpark -> SparkSession
            * getLogger -> logger
* make_mart: 마트 데이터 만드는 기능 실행하는 오브젝트
    * RunAllMart: 지방인허가 요식업 마트, 거주 인구 마트, 국민연금 근로인구 평균급여 마트 기능 실행
    * RunCateringMart: 지방인허가 요식업 마트 기능 실행
    * RunResidentPopulationMart: 거주인구 마트 기능 실행
    * RunSggPensionAmtPopMart: 국민연금 근로인구, 평균급여 마트 기능 실행
* make_score: 지수 로직은 지수 관련 문서 참고
