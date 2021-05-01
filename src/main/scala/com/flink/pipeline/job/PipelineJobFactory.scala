package com.flink.pipeline.job

import com.flink.pipeline.job.impl.PipelineJobImpl
import com.flink.pipeline.model.PipelineRule
import com.flink.pipeline.profile.Profile
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Properties

object PipelineJobFactory {

  implicit class MapWrapper(map: Map[String, Any]) {
    def toProperties: Properties = {
      new Properties() {
        map.foreach(tuple => this.setProperty(tuple._1, tuple._2.toString))
      }
    }
  }

  def createPipelineJob(rule: PipelineRule, profile: Profile, env: StreamExecutionEnvironment): PipelineJob = {
    val jobContext = PipelineJobContext(
      streamEnv = env,
      rule = rule,
      kafkaProducerOpts = profile.getKafkaProducerConfig.toProperties,
      kafkaConsumerOpts = profile.getKafkaConsumerConfig.toProperties,
      schemaRegistry = profile.getSchemaRegistryConfig
    )

    new PipelineJobImpl(jobContext)
  }
}
