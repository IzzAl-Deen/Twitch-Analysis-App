package consumer.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQuery

object SaveToMongo {
  def Save(DF : DataFrame, formatType : String): StreamingQuery ={
    val windowQuery = DF.writeStream
      .outputMode(formatType)
      .format("console")
      .option("truncate", false)
      .option("numRows", 15)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    windowQuery

  }

}
