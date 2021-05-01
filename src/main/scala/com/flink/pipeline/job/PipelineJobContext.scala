package com.flink.pipeline.job

import com.flink.pipeline.model.PipelineRule
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Properties

case class PipelineJobContext(streamEnv: StreamExecutionEnvironment,
                              rule: PipelineRule,
                              kafkaProducerOpts: Properties,
                              kafkaConsumerOpts: Properties,
                              schemaRegistry: Option[Map[String, Any]])
