package com.flink.pipeline.job.impl

import com.flink.pipeline.constant.Constants.{CONST_AVRO, CONST_CSV, CONST_FILE, CONST_JSON, CONST_KAFKA, CONST_LOG_TYPE}
import com.flink.pipeline.job.{PipelineJob, PipelineJobContext}
import com.flink.pipeline.model.Conversion
import com.flink.pipeline.sink.CSVSink
import com.flink.pipeline.source.CSVSource
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, `tableConversions`}


private[job] class PipelineJobImpl(jobCtx: PipelineJobContext) extends PipelineJob {

  private lazy val tableEnv: StreamTableEnvironment = {
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    StreamTableEnvironment.create(jobCtx.streamEnv, bsSettings)
  }

  override def execute(): Unit = {
    sourceTask andThen transTask andThen sinkTask apply Unit
  }

  private val sourceTask = new Function[Unit, Unit] {
    override def apply(v1: Unit): Unit = {
      println(s"${jobCtx.rule.source.`type`}, ${jobCtx.rule.source.format}")

      (jobCtx.rule.source.`type`, jobCtx.rule.source.format) match {
        case (CONST_KAFKA, CONST_AVRO) => kafkaAvroSource()
        case (CONST_KAFKA, CONST_JSON) => kafkaJsonSource()
        case (CONST_FILE, CONST_CSV) => fileCSVSource()
        case (CONST_FILE, CONST_JSON) => fileJsonSource()
        case (_, _) => println("to be implemented...")
      }
    }
  }

  private val transTask = new Function[Unit, Table] {
    override def apply(placeholder: Unit): Table = {
      trigger()
    }
  }

  private val sinkTask = new Function[Table, Unit] {
    override def apply(queryTable: Table): Unit = {
      jobCtx.rule.sinks.foreach(sink => {
        (sink.`type`, sink.format) match {
          case (CONST_KAFKA, CONST_AVRO) => kafkaAvroSink()
          case (CONST_KAFKA, CONST_JSON) => kafkaJsonSink()
          case (CONST_FILE, CONST_CSV) => fileCSVSink(queryTable)
          case (CONST_FILE, CONST_JSON) => fileJsonSink()
          case (_, _) => println("to be implemented...")
        }
      })
    }
  }

  // source functions
  private def kafkaAvroSource(): Unit = ???

  private def kafkaJsonSource(): Unit = ???

  private def fileCSVSource(): Unit = {
    val csvFilePath = jobCtx.rule.source.path
    val ds = jobCtx.streamEnv.addSource(new CSVSource(csvFilePath))

    val logType = jobCtx.rule.source.opts(CONST_LOG_TYPE)
    tableEnv.createTemporaryView(logType, ds)
  }

  private def fileJsonSource(): Unit = ???

  // transformation
  private def trigger(): Table = {
    val queryTable = tableEnv.sqlQuery(jobCtx.rule.transformation.trigger)
    queryTable.printSchema()
    queryTable
  }

  // sink functions
  private def kafkaAvroSink(): Unit = ???

  private def kafkaJsonSink(): Unit = ???

  private def fileCSVSink(queryTable: Table): Unit = {
    jobCtx.rule.sinks.foreach(sink => {
      sink.schema match {
        case "com.flink.pipeline.model.Conversion" => {
          val sinkStream = queryTable.toAppendStream[Conversion]
          sinkStream.print()

          sinkStream.addSink(new CSVSink(sink.path))
        }
        case _ => {
          println("to be implemented...")
        }
      }
    })
  }

  private def fileJsonSink(): Unit = ???
}
