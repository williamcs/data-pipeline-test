package com.flink.example.broadcast.process

import com.flink.pipeline.model.Conversion
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

class ConversionProcessFunction extends BroadcastProcessFunction[Conversion, String, Conversion] {

  val descriptor = new MapStateDescriptor[String, String]("dynamicConfig", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

  override def processElement(value: Conversion, ctx: BroadcastProcessFunction[Conversion, String, Conversion]#ReadOnlyContext, out: Collector[Conversion]): Unit = {
    val configMap: ReadOnlyBroadcastState[String, String] = ctx.getBroadcastState(descriptor)
    val xyz_campaign_id = value.xyz_campaign_id.toString
    val gender = value.gender

    if (configMap.contains(xyz_campaign_id) && configMap.get(xyz_campaign_id).equals(gender)) {
      out.collect(value)
    }
  }

  override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[Conversion, String, Conversion]#Context, out: Collector[Conversion]): Unit = {
    val configMap = ctx.getBroadcastState(descriptor)

    val tokens = value.split(",")
    configMap.put(tokens(0), tokens(1))
  }
}
