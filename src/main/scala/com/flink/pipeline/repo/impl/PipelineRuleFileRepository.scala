package com.flink.pipeline.repo.impl

import com.flink.pipeline.constant.Constants.CONST_URL
import com.flink.pipeline.model.PipelineRule
import com.flink.pipeline.model.PipelineRule.JValueToRule
import com.flink.pipeline.repo.PipelineRuleRepository
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.jackson.JsonMethods.parse

import java.net.URL
import scala.io.Source

class PipelineRuleFileRepository(props: Map[String, Any]) extends PipelineRuleRepository {

  override def getAllPipelineRules: List[PipelineRule] = {
    val bufferedSource = Source.fromURL(new URL(props(CONST_URL).toString))

    val jValue = parse(bufferedSource.getLines().mkString("\n"))

    println("jValue: " + jValue)

    bufferedSource.close()

    jValue match {
      case JArray(arr) => {
        arr.map(rule => rule.toRule)
      }
      case obj@JObject(_) => {
        List(obj.toRule)
      }
    }
  }
}
