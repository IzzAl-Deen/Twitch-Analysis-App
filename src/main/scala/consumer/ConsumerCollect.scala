package consumer

import config.KafkaConfig._
import config.SparkConfig._
import consumer.helper.{PeriodTopUsers, SaveToMongo, TopUsers}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ConsumerCollect {
  def RunConsumer(): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println(s"Connecting to Kafka at $kafkaLocalHost")
    println(s"Subscribing to topic: $topic")

    val streamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaLocalHost)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val messageSchema = new StructType()
      .add("timestamp", StringType)
      .add("user", StringType)
      .add("message", StringType)

    val parsedStream = streamDF
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", messageSchema).as("data"))
      .select("data.*")
      .filter($"user".isNotNull && $"user" =!= "unknown User")

    val timestampDF = parsedStream
      .withColumn("ts", to_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss"))
      .filter($"ts".isNotNull)

    val topUsers = TopUsers.Run(timestampDF)
    val periodTopUsers = PeriodTopUsers.Run(timestampDF)

    SaveToMongo.Save(topUsers, "complete")
    SaveToMongo.Save(periodTopUsers, "complete")



    spark.streams.awaitAnyTermination()
  }
}