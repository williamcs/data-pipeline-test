package com.flink.pipeline.model

import com.flink.pipeline.constant.Constants
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JArray, JNothing, JNull, JObject, JString, JValue}

case class PipelineRule(name: String, source: PipelineRuleSource, transformation: PipelineRuleTransformation, sinks: List[PipelineRuleSink])

case class PipelineRuleSource(`type`: String, format: String, schema: String, path: String, opts: Map[String, String])

case class PipelineRuleTransformation(trigger: String)

case class PipelineRuleSink(`type`: String, format: String, schema: String, path: String, opts: Map[String, String])

object PipelineRule {

  private val sourceOpts = List(Constants.CONST_TOPIC, Constants.CONST_LOG_TYPE, Constants.CONST_EVENT_TIME_ATTR)
  private val sinkOpts = List(Constants.CONST_TOPIC, Constants.CONST_PATH)

  implicit class JValueToRule(json: JValue) {
    implicit val formats: DefaultFormats.type = DefaultFormats

    def toRule: PipelineRule = {
      PipelineRule(
        name = (json \ Constants.JFIELD_NAME).extract[String],
        source = toSource(json \ Constants.JFIELD_SOURCE),
        transformation = toTransformation(json \ Constants.JFIELD_TRIGGER),
        sinks = toSinks(json \ Constants.JFIELD_SINKS)
      )
    }

  }

  def toSource(json: json4s.JValue): PipelineRuleSource = {
    implicit val format: DefaultFormats.type = DefaultFormats

    PipelineRuleSource(
      `type` = (json \ Constants.CONST_TYPE).extract[String],
      format = (json \ Constants.CONST_FORMAT).extract[String],
      schema = (json \ Constants.CONST_SCHEMA).extract[String],
      path = (json \ Constants.CONST_PATH).extract[String],
      sourceOpts.foldLeft(Map.empty[String, String])((map, opt) => {
        json \ opt match {
          case JNothing | JNull => map
          case JString(s) => map + (opt -> s)
          case _ => map + (opt -> json.extract[String])
        }
      })
    )
  }

  def toTransformation(json: JValue): PipelineRuleTransformation = {
    implicit val format: DefaultFormats.type = DefaultFormats

    PipelineRuleTransformation(
      trigger = json.extract[String]
    )
  }

  def toSinks(json: JValue): List[PipelineRuleSink] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    def toSink(json: JValue): PipelineRuleSink = {
      PipelineRuleSink(
        `type` = (json \ Constants.CONST_TYPE).extract[String],
        format = (json \ Constants.CONST_FORMAT).extract[String],
        schema = (json \ Constants.CONST_SCHEMA).extract[String],
        path = (json \ Constants.CONST_PATH).extract[String],
        opts = sinkOpts.foldLeft(Map.empty[String, String])((map, opt) => {
          json \ opt match {
            case JNull | JNothing => map
            case JString(s) => map + (opt -> s)
            case _ => map + (opt -> json.extract[String])
          }
        })
      )
    }

    json match {
      case JArray(arr) => {
        arr.map(toSink(_))
      }
      case obj@JObject(_) => {
        List(toSink(obj))
      }
      case _ => throw new Exception("sinks should be either an array or an object ... ")
    }
  }
}
