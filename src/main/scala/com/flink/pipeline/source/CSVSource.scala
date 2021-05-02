package com.flink.pipeline.source

import com.flink.pipeline.model.Conversion
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

class CSVSource(csvFilePath: String) extends SourceFunction[Conversion] {

  @transient
  private var reader: BufferedReader = null

  override def run(ctx: SourceFunction.SourceContext[Conversion]): Unit = {
    val fileStream = new FileInputStream(csvFilePath)
    reader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"))

    generateStringStream(ctx)

    reader.close()
    reader = null
  }

  override def cancel(): Unit = {
    try {
      if (reader != null) {
        reader.close()
      }
    } finally {
      reader = null
    }
  }

  private def generateStringStream(sourceContext: SourceFunction.SourceContext[Conversion]): Unit = {
    if (!reader.ready) return

    // read first line and skip the header
    val firstLine = reader.readLine
    println("skip first line: " + firstLine)

    // read all the following lines
    while (reader.ready) {
      val line = reader.readLine
      val conversion = Conversion.fromString(line)

      sourceContext.collect(conversion)

      Thread.sleep(100)
    }
  }
}
