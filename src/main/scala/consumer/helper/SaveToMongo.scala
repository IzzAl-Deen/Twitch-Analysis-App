package consumer.helper

import com.mongodb.client.model.Aggregates.count
import config.MongoConfig._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}


object SaveToMongo {
  def Save(DF : DataFrame,output: String, collection : String): StreamingQuery = {

    val query = DF.writeStream
      .outputMode(output)
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.write
          .format("mongo")
          .option("uri", uri)
          .option("database", database)
          .option("collection", collection)
          .mode("overwrite")
          .save()
      }
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    query
  }
}
