import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Collections

class StreamingJobIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  "IncrementFlatMapFunction pipeline" should "incrementValues" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(2)

    // values are collected in a static variable
    CollectSink.values.clear()

    // create a stream of custom elements and apply transformations
    val input: DataStream[Long] = env.fromCollection(Seq(1L, 21L, 22L))

    input.map(new IncrementMapFunction())
      .addSink(new CollectSink())

    // execute
    env.execute()

    // verify your results
    CollectSink.values should contain allOf(2, 22, 23)
  }
}

// create a testing sink
class CollectSink extends SinkFunction[Long] {

  override def invoke(value: Long, context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
  }
}

object CollectSink {
  // must be static
  val values: java.util.List[Long] = Collections.synchronizedList(new java.util.ArrayList())
}

class IncrementMapFunction extends MapFunction[Long, Long] {

  override def map(record: Long): Long = {
    record + 1
  }
}
