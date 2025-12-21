package consumer.helper

import config.SparkConfig.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

object MostFrequentWords {

  def Run(DF: DataFrame): DataFrame = {

    val topWords = DF
      .withColumn("word", explode(split(lower($"message"), "\\s+")))
      .filter(length($"word") > 0)
      .groupBy($"word")
      .agg(count("*").as("total_count"))
      .orderBy($"total_count".desc)

    topWords
  }
}