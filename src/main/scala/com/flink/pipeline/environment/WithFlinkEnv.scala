package com.flink.pipeline.environment

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

trait WithFlinkEnv {

  lazy val flinkStreamExecutionEnvironment: StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
  }

  def createStreamTableEnvironment: StreamTableEnvironment = {
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    StreamTableEnvironment.create(flinkStreamExecutionEnvironment, bsSettings)
  }
}
