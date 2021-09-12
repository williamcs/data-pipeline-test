package com.flink.example.wordcount.source

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.io.{BufferedReader, FileReader}
import java.util.concurrent.TimeUnit

class WordCountSource(filePath: String) extends RichSourceFunction[String] {

  @transient
  private var reader: BufferedReader = null
  private var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (isRunning) {
      reader = new BufferedReader(new FileReader(filePath))

      while (reader.ready()) {
        val line = reader.readLine
        ctx.collect(line)
      }

      TimeUnit.SECONDS.sleep(10)
    }
  }

  override def cancel(): Unit = {
    isRunning = false

    try {
      if (reader != null) {
        reader.close()
      }
    } finally {
      reader = null
    }
  }
}
