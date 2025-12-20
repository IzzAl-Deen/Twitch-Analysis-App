package config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  val spark = SparkSession.builder()
    .appName("TwitchChatAnalyzer")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/twitch-checkpoint")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()


}
