name := "at"

version := "0.1"

scalaVersion := "2.11.12"

assemblyJarName in assembly := "at.jar"

// additional libraries
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.apache.spark" % "spark-core_2.11" % "2.4.7" ,
  "org.apache.spark" % "spark-sql_2.11" % "2.4.7" ,
  "org.apache.spark" % "spark-mllib_2.11" % "2.4.7" ,
  "org.apache.spark" % "spark-repl_2.11" % "2.4.7" ,
  "org.apache.spark" % "spark-yarn_2.11" % "2.4.7" ,
  "com.crealytics" % "spark-excel_2.11" % "0.13.1",
  "mysql" % "mysql-connector-java" % "5.1.38",
  "org.jsoup" % "jsoup" % "1.13.1"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case _ => MergeStrategy.first
}
