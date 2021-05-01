package com.flink.pipeline.profile.impl

import com.flink.pipeline.constant.Constants._
import com.flink.pipeline.profile.Profile
import com.flink.pipeline.url.WithClasspathURL
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml

import java.net.URL
import scala.collection.JavaConverters
import scala.util.{Failure, Success, Try}


class YamlProfile(private val url: URL) extends Profile {

  private lazy val config = loadConfig()
  private lazy val kafkaConfig = getKafkaConfig()
  private lazy val schemaRegistry = parseSchemaRegistry()
  private lazy val pipelineRule = parsePipelineRule()

  override def getKafkaConsumerConfig: Map[String, Any] = {
    processKafkaConfig(CONST_CONSUMER)
  }

  override def getKafkaProducerConfig: Map[String, Any] = {
    processKafkaConfig(CONST_PRODUCER)
  }

  override def getSchemaRegistryConfig: Option[Map[String, Any]] = {
    schemaRegistry
  }

  override def getPipelineRuleConfig: Map[String, Any] = {
    pipelineRule
  }

  private def loadConfig(): Map[String, Any] = {
    JavaConverters.mapAsScalaMap(new Yaml().load(url.openStream()).asInstanceOf[java.util.Map[String, Any]]).toMap
  }

  private def getMap(config: Map[String, Any], key: String): Map[String, Any] = {
    JavaConverters.mapAsScalaMap(config(key).asInstanceOf[java.util.Map[String, Any]]).toMap
  }

  private def getString(config: Map[String, Any], key: String): String = {
    config(key).toString
  }

  private def getKafkaConfig(): Map[String, Any] = {
    getMap(config, CONST_KAFKA)
  }

  private def parseSchemaRegistry(): Option[Map[String, Any]] = {
    Option(System.getenv(ENV_SCHEMA_REGISTRY_URL)) match {
      case Some(url) => {
        Some(Map(CONST_URL -> url))
      }
      case None => {
        Try(getMap(config, CONST_SCHEMA_REGISTRY)) match {
          case Success(value) => Some(value)
          case Failure(exception) => None
        }
      }
    }
  }

  private def processKafkaConfig(path: String): Map[String, Any] = {
    (Option(System.getenv(ENV_KAFKA_URL)), Option(System.getenv(ENV_ZOOKEEPER_URL))) match {
      case (Some(k), Some(z)) => {
        println(("Both Kafka and Zookeeper are set ... "))
        val map = getMap(kafkaConfig, path) + ("bootstrap.servers" -> k) + ("zookeeper.connect" -> z)
        map.foreach(tuple => println(s"${tuple._1}:${tuple._2.toString}"))
        map
      }
      case (Some(k), None) => {
        getMap(kafkaConfig, path) + ("bootstrap.servers" -> k)
      }
      case (None, Some(z)) => {
        getMap(kafkaConfig, path) + ("zookeeper.connect" -> z)
      }
      case (None, None) => {
        getMap(kafkaConfig, path)
      }
    }
  }

  private def parsePipelineRule(): Map[String, Any] = {
    Try(getMap(config, CONST_PIPELINE_RULE)) match {
      case Success(value) => value
      case Failure(exception) => Map(CONST_URL -> DEFAULT_RULE_DEFINITION_URL)
    }
  }

}

object YamlProfile extends WithClasspathURL {
  def main(args: Array[String]): Unit = {
    val url = new URL(DEFAULT_PROFILE_YAML_URL)
    val profile = new YamlProfile(url)

    println(profile.pipelineRule)
  }
}
