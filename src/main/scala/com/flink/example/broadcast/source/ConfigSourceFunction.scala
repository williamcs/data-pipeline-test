package com.flink.example.broadcast.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

class ConfigSourceFunction extends SourceFunction[String] {

  var flag = true

  val configList = List("1178,F")

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (flag) {
      for (value <- configList) {
        ctx.collect(value)
      }
    }

    Thread.sleep(60 * 1000)
  }

  override def cancel(): Unit = {
    flag = false
  }
}
