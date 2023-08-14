/*
jdbc Properties

*/
package common

package object jdbc {
  //  localData
  def localDataJdbcProp() = {
    val username = ""
    val password = ""
    val url = "jdbc:mysql://127.0.0.1:3307/CLCTDATA?serverTimezone=UTC&autoReconnect=true"

    val connProp = new java.util.Properties()
    connProp.put("user", username)
    connProp.put("password", password)
    connProp.put("driver", "com.mysql.jdbc.Driver")
    (url, connProp)
  }

  //  publicData
  def publicDataJdbcProp() = {
    val username = ""
    val password = ""
    val url = "jdbc:mysql://127.0.0.1:3307/CLCTDATA?serverTimezone=UTC&autoReconnect=true"

    val connProp = new java.util.Properties()
    connProp.put("user", username)
    connProp.put("password", password)
    connProp.put("driver", "com.mysql.jdbc.Driver")
    (url, connProp)
  }


  def martDataJdbcProp() = {
    val username = ""
    val password = ""
    val url = "jdbc:mysql://127.0.0.1:3307/AT?serverTimezone=UTC&autoReconnect=true"

    val connProp = new java.util.Properties()
    connProp.put("user", username)
    connProp.put("password", password)
    connProp.put("driver", "com.mysql.jdbc.Driver")
    (url, connProp)
  }

  //  인구 지수 중 잠재지수 값 있는 DB
  def potentialPopJdbcProp() = {
    val username = ""
    val password = ""
    val url = "jdbc:mysql://127.0.0.1:3307/SM?serverTimezone=UTC&autoReconnect=true"

    val connProp = new java.util.Properties()
    connProp.put("user", username)
    connProp.put("password", password)
    connProp.put("driver", "com.mysql.jdbc.Driver")
    (url, connProp)
  }

  // 지수 결과 저장 DB
  def scoreJdbcPop() = {
    val username = ""
    val password = ""
    val url = "jdbc:mysql://0.0.0.0/aatbc_db?serverTimezone=UTC&autoReconnect=true"

    val connProp = new java.util.Properties()
    connProp.put("user", username)
    connProp.put("password", password)
    connProp.put("driver", "com.mysql.jdbc.Driver")
    (url, connProp)
  }
}