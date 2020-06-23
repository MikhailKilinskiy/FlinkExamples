package org.flinkexample

import org.apache.avro.generic.GenericRecord
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.flinkexample.utils.KafkaAvroReader
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark


object AppTransformation extends Serializable {

  def main(args: Array[String]): Unit = {

    val (source, env) = CreateSource.create()

    val filteredSource = source
      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks)
      .filter(r => r.get("EventValue").asInstanceOf[Int] >= 10)


    val resultStream = filteredSource
      .map{r => (r.get("EventId").asInstanceOf[Int], r.get("EventValue").asInstanceOf[Int])}
        .keyBy(0)
        .sum(1)


    resultStream.print().setParallelism(1)

    env.execute("Test")
  }

}

class MyTimestampsAndWatermarks extends AssignerWithPeriodicWatermarks[GenericRecord] {

  var ts: Long = Long.MinValue

  override def extractTimestamp(element: GenericRecord, previousElementTimestamp: Long): Long = {
    val eventTs = element.get("EventTime").asInstanceOf[Long]
    ts = eventTs

    eventTs
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(ts - 1000)
  }

}