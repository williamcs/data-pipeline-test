import org.apache.flink.api.common.functions.MapFunction
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IncrementMapFunctionTest extends AnyFlatSpec with Matchers {

  "IncrementMapFunction" should "increment values" in {
    // instantiate your function
    val incrementer: SimpleMapFunction = new SimpleMapFunction()

    // call the methods that you have implemented
    incrementer.map(2) should be(3)
  }
}

class SimpleMapFunction extends MapFunction[Long, Long] {

  override def map(record: Long): Long = {
    record + 1
  }
}