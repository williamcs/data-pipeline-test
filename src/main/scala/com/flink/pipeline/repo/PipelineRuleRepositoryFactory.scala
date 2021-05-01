package com.flink.pipeline.repo

import com.flink.pipeline.constant.Constants.CONST_TYPE
import com.flink.pipeline.repo.impl.{PipelineRuleFileRepository, PipelineRuleHttpRepository}

object PipelineRuleRepositoryFactory {

  def getPipelineRuleRepository(props: Map[String, Any]): PipelineRuleRepository = {
    props.get(CONST_TYPE) match {
      case Some(t) =>
        t match {
          case "http" => new PipelineRuleHttpRepository(props)
          case "classpath" => new PipelineRuleFileRepository(props)
          case _ => throw new Exception(s"'$t' type is not supported in pipeline rule configuration")
        }
      case None => throw new Exception(s"'$CONST_TYPE' field is a must-have in pipeline rule configuration")
    }
  }

}
