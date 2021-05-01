package com.flink.pipeline.repo.impl

import com.flink.pipeline.constant.Constants.CONST_URL
import com.flink.pipeline.model.PipelineRule
import com.flink.pipeline.model.PipelineRule.JValueToRule
import com.flink.pipeline.repo.PipelineRuleRepository
import org.asynchttpclient.Dsl.asyncHttpClient
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.parse

class PipelineRuleHttpRepository(props: Map[String, Any]) extends PipelineRuleRepository {

  private lazy val asyncClient = asyncHttpClient()

  override def getAllPipelineRules: List[PipelineRule] = {
    val response = asyncClient.prepareGet(s"${props.get(CONST_URL).get.toString}")
      .addHeader("Accept", "application/json")
      .execute().get().getResponseBody

    val jValue = parse(response)

    jValue match {
      case JArray(arr) => {
        arr.map(_.toRule)
      }
      case _ => throw new Exception(s"GET operation should return an array of rules")
    }
  }
}
