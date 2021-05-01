package com.flink.pipeline.bootstrap

import com.flink.pipeline.environment.WithFlinkEnv
import com.flink.pipeline.job.PipelineJobFactory
import com.flink.pipeline.model.PipelineRule
import com.flink.pipeline.profile.WithProfile
import com.flink.pipeline.repo.{PipelineRuleRepository, PipelineRuleRepositoryFactory}
import com.flink.pipeline.url.WithClasspathURL

object BootstrapMain extends App with WithClasspathURL with WithProfile with WithFlinkEnv {

  val repo: PipelineRuleRepository = PipelineRuleRepositoryFactory.getPipelineRuleRepository(profile.getPipelineRuleConfig)

  val rules: List[PipelineRule] = repo.getAllPipelineRules

  rules.foreach(rule => {
    //    println(rule)
    PipelineJobFactory.createPipelineJob(rule, profile, flinkStreamExecutionEnvironment).execute()
  })

  println("running the jobs...")

  flinkStreamExecutionEnvironment.execute("flink data pipeline test")
}
