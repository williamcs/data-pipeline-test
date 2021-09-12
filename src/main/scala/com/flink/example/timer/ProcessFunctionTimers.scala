package com.flink.example.timer

import com.flink.example.timer.model.SensorReading
import com.flink.example.timer.source.SensorSource
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

/**
 * https://github.com/streaming-with-flink/examples-scala/blob/master/src/main/scala/io/github/streamingwithflink/chapter6/ProcessFunctionTimers.scala
 */
object ProcessFunctionTimers {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val readings: DataStream[SensorReading] = env.addSource(new SensorSource)

    val warnings = readings
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    warnings.print()

    env.execute("Monitor sensor temperatures.")
  }
}

class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, (String, SensorReading)] {

  // hold temperature of last sensor reading
  lazy val lastTemp: ValueState[Double] =
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

  // hold timestamp of currently active timer
  lazy val currentTimer: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  lazy val currSensorReading: ValueState[SensorReading] =
    getRuntimeContext.getState(new ValueStateDescriptor[SensorReading]("currSensorReading", Types.of[SensorReading]))


  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, (String, SensorReading)]#Context,
                              out: Collector[(String, SensorReading)]): Unit = {
    // get previous temperature
    val prevTemp = lastTemp.value()
    // update last temperature
    lastTemp.update(value.temperature)

    val curTimerTimestamp = currentTimer.value()

    if (prevTemp == 0.0) {
      // first sensor reading for this key.
      // we cannot compare it with a previous value.
    } else if (value.temperature < prevTemp) {
      // temperature decreased. Delete current timer.
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      currentTimer.clear()
    } else if (value.temperature > prevTemp && curTimerTimestamp == 0) {
      // temperature increased and we have not set a timer yet.
      // set timer for now + 1 second
      val timerTs = ctx.timerService().currentProcessingTime() + 1000
      ctx.timerService().registerProcessingTimeTimer(timerTs)

      // remember current timer
      currentTimer.update(timerTs)

      currSensorReading.update(value)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, (String, SensorReading)]#OnTimerContext,
                       out: Collector[(String, SensorReading)]): Unit = {

    val sensorReading: SensorReading = currSensorReading.value()
    currSensorReading.clear()

    out.collect(("Temperature of sensor '" + ctx.getCurrentKey + "' monotonically increased for 1 second, with timestamp: " + timestamp, sensorReading))

    // reset current timer
    currentTimer.clear()
  }
}
