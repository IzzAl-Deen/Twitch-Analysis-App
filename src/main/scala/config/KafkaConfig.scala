package config

import org.apache.kafka.clients.producer.KafkaProducer

import java.util.Properties

object KafkaConfig {
  val kafkaLocalHost = "localhost:9092"
  val topic = "twitch-chat"

  val props = new Properties()
  props.put("bootstrap.servers", kafkaLocalHost)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

}
