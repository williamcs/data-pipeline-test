package com.flink.example.wordcount.process

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class WordCountKeyedProcessFunction extends KeyedProcessFunction[String, (String, Int), (String, Int)] {

  private var mapState: MapState[String, Int] = _
  private var valueState: ValueState[Int] = _

  override def open(parameters: Configuration): Unit = {
    val mapStateDescriptor = new MapStateDescriptor[String, Int]("mapStateDescriptor", Types.of[String], Types.of[Int])
    mapState = getRuntimeContext.getMapState(mapStateDescriptor)

    val valueStateDescriptor = new ValueStateDescriptor[Int]("valueStateDescriptor", Types.of[Int])
    valueState = getRuntimeContext.getState(valueStateDescriptor)
  }

  override def processElement(value: (String, Int),
                              ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#Context,
                              out: Collector[(String, Int)]): Unit = {
    println("valueState.value(): " + valueState.value())

    if (valueState.value() == null) {
      ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 2000)
    }

    val count = if (mapState.contains(value._1)) mapState.get(value._1) else 0
    mapState.put(value._1, count + 1)

    out.collect((value._1, count + 1))
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#OnTimerContext,
                       out: Collector[(String, Int)]): Unit = {
    mapState.clear()
    valueState.clear()
  }

}
