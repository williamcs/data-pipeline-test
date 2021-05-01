package com.flink.pipeline.source

import com.flink.example.table.StreamSQLExample.Order
import org.apache.flink.streaming.api.functions.source.SourceFunction

class OrderSource extends SourceFunction[Order] {

  override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
    val order =  Order(1L, "beer", 3)

    while (true) {
      ctx.collect(order)

      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    println("Order data source has been canceled.")
  }
}
