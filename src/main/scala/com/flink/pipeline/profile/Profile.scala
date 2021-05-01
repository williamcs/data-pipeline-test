package com.flink.pipeline.profile

trait Profile {

  def getKafkaConsumerConfig: Map[String, Any]

  def getKafkaProducerConfig: Map[String, Any]

  def getSchemaRegistryConfig: Option[Map[String, Any]]

  def getPipelineRuleConfig: Map[String, Any]

}
