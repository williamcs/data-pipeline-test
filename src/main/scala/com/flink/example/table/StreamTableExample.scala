package com.flink.example.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object StreamTableExample {

  def main(args: Array[String]): Unit = {
    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, bsSettings)

    val orderA = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2))).toTable(tEnv)

    val orderB = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1))).toTable(tEnv)

    // union the two tables
    val result: DataStream[Order] = orderA.unionAll(orderB)
      .select('user, 'product, 'amount)
      .where('amount > 2)
      .toAppendStream[Order]

    result.print()

    env.execute()
  }

  case class Order(user: Long, product: String, amount: Int)
}
