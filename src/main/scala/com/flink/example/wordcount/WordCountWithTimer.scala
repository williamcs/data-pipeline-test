package com.flink.example.wordcount

import com.flink.example.wordcount.process.{WordCountFlatMap, WordCountKeyedProcessFunction}
import com.flink.example.wordcount.source.WordCountSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/operators/process_function/
 * https://blog.csdn.net/qq_44962429/article/details/110875555?utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7EBlogCommendFromMachineLearnPai2%7Edefault-12.control&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7EBlogCommendFromMachineLearnPai2%7Edefault-12.control
 */
object WordCountWithTimer {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val filePath = "data/word_count.txt"

    val wordCountSource = env
      .addSource(new WordCountSource(filePath))
      .flatMap(new WordCountFlatMap)
      .keyBy(x => x._1)
      .process(new WordCountKeyedProcessFunction)

    wordCountSource.print()

    env.execute("Word Count with Timer Job")
  }
}
