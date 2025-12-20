package consumer.helper
import config.SparkConfig.spark
import org.apache.spark.sql
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
object PeriodTopUsers {
  import spark.implicits._

  def Run(DF: DataFrame): DataFrame = {

    val top = DF
      .withWatermark("ts", "5 minutes")
      .groupBy(
        window($"ts", "2 minutes"), // 2-minute tumbling window
        $"user"
      )
      .agg(count("*").as("message_count"))
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"user",
        $"message_count"
      )
      .orderBy($"message_count".desc)
    top
  }

}
