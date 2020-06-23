package org.flinkexample

import org.apache.avro.generic.GenericRecord
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.core.fs.Path
import java.net.URI

import org.flinkexample.utils.KafkaAvroReader


object AppSaveToParquet extends Serializable {

  def main(args: Array[String]): Unit = {

    val (source, env) = CreateSource.create()

    val avroSchema = CreateSource.schema

    val sink: StreamingFileSink[GenericRecord] = StreamingFileSink
        .forBulkFormat(new Path(new URI("file:/home/makilins/Downloads/test")),
          ParquetAvroWriters.forGenericRecord(avroSchema))
        .withBucketAssigner(new DateTimeBucketAssigner())
        .build()


    source.addSink(sink)

    env.execute("Test")
  }

}

