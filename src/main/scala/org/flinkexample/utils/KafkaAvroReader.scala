package org.flinkexample.utils

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties


class KafkaAvroReader(env: StreamExecutionEnvironment,
                      topic: String,
                      schemaRegistryUrl: String,
                      subject: String,
                      props: Properties) {

  var avroSchema: Schema = null

  @transient private lazy val schemaRegistry = {

    import scala.collection.JavaConverters._
    val urls = schemaRegistryUrl.split(",").toList.asJava
    val cacheCapacity = 128

    new CachedSchemaRegistryClient(urls, cacheCapacity)
  }

  private def getSchemaAndId(): String = {
    val schemaId = schemaRegistry.getLatestSchemaMetadata(subject).getId
    schemaRegistry.getByID(schemaId).toString
  }

  private def initializeSchema(): Unit = {
    if (avroSchema == null) {
      avroSchema = new Schema.Parser().parse(getSchemaAndId())
    }
  }

  def getSchema: Schema = {
    initializeSchema()

    avroSchema
  }

  def read(): DataStream[GenericRecord] = {

    initializeSchema()

    env
      .addSource(
        new FlinkKafkaConsumer[GenericRecord](
          topic,
          ConfluentRegistryAvroDeserializationSchema.forGeneric(avroSchema, schemaRegistryUrl),
          props
        ).setStartFromEarliest())
  }

}


object KafkaAvroReader{
  def apply(env: StreamExecutionEnvironment,
            topic: String,
            schemaRegistryUrl: String,
            subject: String,
            props: Properties): KafkaAvroReader = new KafkaAvroReader(env, topic, schemaRegistryUrl, subject, props)
}