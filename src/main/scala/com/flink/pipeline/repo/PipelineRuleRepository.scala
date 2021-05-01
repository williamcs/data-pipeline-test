package com.flink.pipeline.repo

import com.flink.pipeline.model.PipelineRule

trait PipelineRuleRepository {

  def getAllPipelineRules: List[PipelineRule]

}
