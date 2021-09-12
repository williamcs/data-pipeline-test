package com.flink.example.wordcount.process

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class WordCountFlatMap extends RichFlatMapFunction[String, (String, Int)] {

  override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
    val split: Array[String] = value.split("\\s+")
    split.foreach(w => out.collect(w, 1))
  }
}
