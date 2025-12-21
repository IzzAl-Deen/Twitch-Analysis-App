package consumer.helper

import config.SparkConfig.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

object PeriodMostFrequentWords {
  def Run(DF: DataFrame): DataFrame = {
    val wordsDF = DF
      .withWatermark("ts", "5 minutes")
      .withColumn("word", explode(split(lower($"message"), "\\s+")))
      .filter(length($"word") > 0)

    val topWords = wordsDF
      .groupBy(
        window($"ts", "2 minutes"),
        $"word"
      )
      .agg(count("*").as("word_count"))
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"word",
        $"word_count"
      )
      .orderBy($"word_count".desc)

    topWords
  }
}
