package com.flink.example.table

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, tableConversions}

object WordCountSQL {

  private lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private lazy val tEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)

  def main(args: Array[String]): Unit = {
    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))

    // register the DataSet as a view "WordCount"
    tEnv.createTemporaryView("WordCount", input, $"word", $"frequency")

    // run a SQL query on the Table and retrieve the result as a new Table
    val table = tEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")

    table.toDataSet[WC].print()
  }

  case class WC(word: String, frequency: Long)
}
