import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable


// https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/testing/
// https://flink.apache.org/news/2020/02/07/a-guide-for-unit-testing-in-apache-flink.html
// https://github.com/dataArtisans/flink-cookbook/blob/master/src/test/scala/exampletest/ExampleTest.scala

class StatefulFlatMapFunctionTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new mutable.Stack[Int]
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new mutable.Stack[String]
    assertThrows[NoSuchElementException] {
      emptyStack.pop()
    }
  }

}
