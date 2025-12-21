package consumer.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import config.SparkConfig._

object FunPercentage {
  import spark.implicits._

  def Run(DF: DataFrame): DataFrame = {

    val targetWords = Seq("lol", "lmao", "hahaha", "lo", "XD")

    val wordsDF = DF
      .withColumn("word", explode(split(lower($"message"), "\\s+")))
      .filter(length($"word") > 0)

    val aggDF = wordsDF
      .groupBy(window($"timestamp", "2 minutes"))
      .agg(
        count("*").as("total_words"),
        sum(when($"word".isin(targetWords: _*), 1).otherwise(0)).as("fun_words")
      )
      .withColumn("percentage", round($"fun_words" * 100.0 / $"total_words", 2))
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"total_words",
        $"fun_words",
        $"percentage"
      )
      .orderBy($"window_start")

    aggDF
  }
}
