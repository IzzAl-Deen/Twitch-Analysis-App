package producer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import producer.helper.TwitchIRC
import producer.helper.ExtractUser
import producer.helper.ExtractMessage
import producer.helper.ExtractTimestamp
import config.KafkaConfig
import config.KafkaConfig.props
import config.TwitchConfig

object ProducerCollect extends App {
  val producer = new KafkaProducer[String, String](props)
  // Enable Twitch IRC tags (REQUIRED for timestamps)
  TwitchIRC.out.println("CAP REQ :twitch.tv/tags")
  val token = TwitchConfig.token
  val nickname = TwitchConfig.nickname
  val channel = TwitchConfig.channel

  // Authenticate and join channel
  TwitchIRC.out.println(s"PASS $token")
  TwitchIRC.out.println(s"NICK $nickname")
  TwitchIRC.out.println(s"JOIN $channel")

  println(s"Connected to Twitch channel $channel as $nickname")
  while (true) {
    val line = TwitchIRC.in.readLine()
    if (line != null) {
      if (line.startsWith("PING")) {
        TwitchIRC.out.println("PONG :tmi.twitch.tv")
      } else if (line.contains("PRIVMSG")) {

        val timestamp = ExtractTimestamp.extractTimestamp(line)
        val user = ExtractUser.extractUser(line)
        val message = ExtractMessage.extractMessage(line)

        val json =
          s"""
					   |{
					   |  "timestamp": "$timestamp",
					   |  "user": "$user",
					   |  "message": "${message.replace("\"", "\\\"")}"
					   |}
					 """.stripMargin

        producer.send(new ProducerRecord[String, String](KafkaConfig.topic, json))

        println(json)
      }
    }
  }
}
