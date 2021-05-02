package com.flink.pipeline.sink

import com.flink.pipeline.model.Conversion
import com.github.tototoshi.csv.CSVWriter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.io.File

class CSVSink(csvFilePath: String) extends RichSinkFunction[Conversion] {

  var file: File = null
  var writer: CSVWriter = null
  lazy val csvHeader = List("ad_id", "xyz_campaign_id", "fb_campaign_di", "age", "gender", "interest", "Impression", "Clicks", "Spent", "Total_Conversion", "Approved_Conversion")

  override def open(parameters: Configuration): Unit = {
    file = new File(csvFilePath)
    writer = CSVWriter.open(file)

    // write header
    writer.writeRow(csvHeader)
  }

  override def invoke(value: Conversion, context: SinkFunction.Context): Unit = {
    writer.writeRow(List(value.ad_id, value.xyz_campaign_id, value.fb_campaign_id, value.age, value.gender,
      value.interest, value.impressions, value.clicks, value.spent, value.totalConversion, value.approvedConversion))
  }

  override def close(): Unit = {
    writer.close()
  }
}
