name := "data-pipeline-test"

version := "0.1"

scalaVersion := "2.12.13"

val flinkVersion = "1.12.2"
val json4sJacksonVersion = "3.7.0-M6"
val asyncHttpClientVersion = "2.12.1"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-queryable-state-runtime" % flinkVersion,
  "org.apache.flink" % "flink-table" % flinkVersion,
  "org.apache.flink" %% "flink-table-api-scala" % flinkVersion,
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion,
  "org.apache.flink" %% "flink-table-planner" % flinkVersion,
  "org.apache.flink" %% "flink-table-planner-blink" % flinkVersion,
  "org.json4s" %% "json4s-jackson" % json4sJacksonVersion,
  "org.asynchttpclient" % "async-http-client" % asyncHttpClientVersion
)
