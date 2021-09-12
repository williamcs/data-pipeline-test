package com.flink.example

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

object SimpleTest {

  val adIdArrayDescriptor = new MapStateDescriptor[Void, String]("test", BasicTypeInfo.VOID_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val s1 = env.fromCollection(Seq("1"))

    val brcs = s1.broadcast(adIdArrayDescriptor)
    val testStream: DataStream[Int] = env.addSource(new DelaySourceFunction)
    testStream.connect(brcs).process(new ConversionProcessFunction).print()
    env.execute("BroadCastDemo")
  }

  class ConversionProcessFunction extends BroadcastProcessFunction[Int, String, String] {
    val adIdArrayDescriptor = new MapStateDescriptor[Void, String]("test", BasicTypeInfo.VOID_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

    override def processElement(value: Int, ctx: BroadcastProcessFunction[Int, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
      Thread.sleep(1000)

      val idArray = ctx.getBroadcastState(adIdArrayDescriptor).get(null)
      if (idArray == null) {
        println("empty id array")
      }
      if (idArray != null) {
        out.collect(idArray)
      }

      println("value is-------------: " + value)
    }

    override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[Int, String, String]#Context, out: Collector[String]): Unit = {
      ctx.getBroadcastState(adIdArrayDescriptor).put(null, value)
    }
  }

  class DelaySourceFunction() extends SourceFunction[Int] {
    override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
      Thread.sleep(5 * 1000)
      List(1, 2, 3).foreach(i => {
        Thread.sleep(1000)
        ctx.collect(i)
      })
    }

    override def cancel(): Unit = ???
  }
}


