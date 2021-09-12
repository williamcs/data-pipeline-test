package com.flink.example.timer.source

import com.flink.example.timer.model.SensorReading
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading] {

  var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    // initialize sensor ids and temperatures
    var curFTemp = (1 to 10).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    // emit data until being canceled
    while (running) {
      // update temperature
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
      // get current time
      // val currTime = Calendar.getInstance.getTimeInMillis
      val currTime = System.currentTimeMillis()

      // emit new SensorReading
      curFTemp.foreach(t => ctx.collect(SensorReading(t._1, currTime, t._2)))

      // wait for 100 ms
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
