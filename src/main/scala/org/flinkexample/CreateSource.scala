package org.flinkexample

import com.typesafe.config.ConfigFactory
import org.apache.avro.generic.GenericRecord
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import java.util.Properties
import org.apache.avro.Schema

import org.flinkexample.utils.KafkaAvroReader


object CreateSource {

  val conf = ConfigFactory.parseResources("app.conf")
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", conf.getString("bootstrap.servers"))
  properties.setProperty("group.id", conf.getString("group.id"))

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val reader = KafkaAvroReader(
    env,
    conf.getString("kafka.topik"),
    conf.getString("schema_registry_url"),
    conf.getString("subject"),
    properties
  )

  val schema = reader.getSchema

  def create(): (DataStream[GenericRecord], StreamExecutionEnvironment) = {

    val source: DataStream[GenericRecord] = reader.read()

    (source, env)
  }

}
