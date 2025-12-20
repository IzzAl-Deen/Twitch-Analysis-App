package consumer.helper

import config.SparkConfig
import config.SparkConfig.spark
import consumer.ConsumerCollect
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

object TopUsers {

  import spark.implicits._
  def Run(DF : DataFrame): DataFrame = {
    val top = DF
      .groupBy($"user")
      .agg(count("*").as("total_messages"))
      .orderBy($"total_messages".desc)

    top
  }
}
