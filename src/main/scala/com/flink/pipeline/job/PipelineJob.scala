package com.flink.pipeline.job

trait PipelineJob extends (() => Unit) {

  def execute(): Unit

  override def apply(): Unit = execute()
}
