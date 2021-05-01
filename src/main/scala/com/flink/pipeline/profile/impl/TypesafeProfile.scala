package com.flink.pipeline.profile.impl

import com.flink.pipeline.constant.Constants
import com.flink.pipeline.constant.Constants.{CONST_URL, PIPELINE_RULE, DEFAULT_RULE_DEFINITION_URL, KAFKA_CONSUMER, KAFKA_PRODUCER, SCHEMA_REGISTRY}
import com.flink.pipeline.profile.Profile
import com.flink.pipeline.url.WithClasspathURL
import com.typesafe.config.{Config, ConfigFactory}

import java.net.URL
import scala.collection.JavaConverters
import scala.util.{Failure, Success, Try}

class TypesafeProfile(private val url: URL) extends Profile {

  private lazy val config = loadConfig()
  private lazy val kafkaConsumer = getKafkaConsumer
  private lazy val kafkaProducer = getKafkaProducer
  private lazy val schemaRegistry = parseSchemaRegistry
  private lazy val pipelineRule = parsePipelineRule

  override def getKafkaConsumerConfig: Map[String, Any] = kafkaConsumer

  override def getKafkaProducerConfig: Map[String, Any] = kafkaProducer

  override def getSchemaRegistryConfig: Option[Map[String, Any]] = schemaRegistry

  override def getPipelineRuleConfig: Map[String, Any] = pipelineRule

  private def loadConfig(): Config = {
    ConfigFactory.parseURL(url)
  }

  private def getMapThroughPath(path: String): Map[String, Any] = {
    JavaConverters.asScalaSet(config.getConfig(path).entrySet()).map(entry => {
      (entry.getKey, entry.getValue.unwrapped())
    }).toMap
  }

  private def getKafkaConsumer: Map[String, Any] = {
    getMapThroughPath(KAFKA_CONSUMER)
  }

  private def getKafkaProducer: Map[String, Any] = {
    getMapThroughPath(KAFKA_PRODUCER)
  }

  private def parseSchemaRegistry: Option[Map[String, Any]] = {
    Try(getMapThroughPath(SCHEMA_REGISTRY)) match {
      case Success(value) => Some(value)
      case Failure(_) => None
    }
  }

  private def parsePipelineRule: Map[String, Any] = {
    Try(getMapThroughPath(PIPELINE_RULE)) match {
      case Success(value) => value
      case Failure(_) => Map(CONST_URL -> DEFAULT_RULE_DEFINITION_URL)
    }
  }

}

object TypesafeProfile extends WithClasspathURL {

  def main(args: Array[String]): Unit = {
    val url = new URL(Constants.DEFAULT_PROFILE_CONF_URL)
    val profile = new TypesafeProfile(url)

    println(profile.kafkaProducer)
  }
}
