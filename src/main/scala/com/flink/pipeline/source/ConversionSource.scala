package com.flink.pipeline.source

import com.flink.pipeline.model.Conversion
import org.apache.flink.streaming.api.functions.source.SourceFunction

class ConversionSource extends SourceFunction[Conversion] {

  override def run(ctx: SourceFunction.SourceContext[Conversion]): Unit = {
    val conversion = Conversion(1L, 1L, 1L, "23", "male", 8, 9, 100, 199.9, 100, 50)

    while (true) {
      ctx.collect(conversion)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    println("Conversion data source has been canceled.")
  }
}
