package com.flink.example.broadcast

import com.flink.example.broadcast.process.ConversionProcessFunction
import com.flink.example.broadcast.source.ConfigSourceFunction
import com.flink.pipeline.source.CSVSource
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object BroadCastDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(1)
    env.setParallelism(1)

    val csvFilePath = "data/KAG_conversion_data.csv"
    val initFilePath = "data/broad_cast.txt"
    val initStream = env.readTextFile(initFilePath)

    val descriptor = new MapStateDescriptor[String, String]("dynamicConfig", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

    val configStream: BroadcastStream[String] = env.addSource(new ConfigSourceFunction).union(initStream).broadcast(descriptor)

    val connectedStream = env.addSource(new CSVSource(csvFilePath)).connect(configStream)

    connectedStream.process(new ConversionProcessFunction).print()

    env.execute("BroadCastDemo")
  }
}
