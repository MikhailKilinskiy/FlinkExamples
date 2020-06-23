package org.flinkexample

import java.net.URI

import org.apache.avro.generic.GenericRecord
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

import java.util.concurrent.TimeUnit

object AppSaveToFile extends Serializable {

  def main(args: Array[String]): Unit = {

    val (source, env) = CreateSource.create()

    val avroSchema = CreateSource.schema

    val outputPath = "file:/home/makilins/Downloads/test"
    val maxFileSizeMb = 1024 * 1024 * 1024
    val fileRollingIntervalSeconds = TimeUnit.MINUTES.toMillis(15)
    val checkpointInterval = TimeUnit.MINUTES.toMillis(5)

    val resultStream = source
      .map{r => r.get("EventId").toString + ": " + r.get("EventValue").toString}

    val rollingPolicy = DefaultRollingPolicy
      .create()
      .withRolloverInterval(fileRollingIntervalSeconds)
      .withMaxPartSize(maxFileSizeMb)
      .withInactivityInterval(fileRollingIntervalSeconds)
      .build[String, String]()

    val sink: StreamingFileSink[String] = StreamingFileSink.forRowFormat(new Path(outputPath),
      new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(rollingPolicy)
      .build()

    resultStream.addSink(sink)



    env.execute("Test")
  }

}

