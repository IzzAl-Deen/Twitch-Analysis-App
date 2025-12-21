package consumer.probabilistic

import config.MongoConfig._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object ProbabilisticMongo {

  def Save(DF: DataFrame, collection: String): StreamingQuery = {
    var prevTop5: Seq[(String, Long)] = Seq()

    val cmsSave = DF.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        if (!batchDF.isEmpty) {
          val currentTop5 = CountMinSketch.Run(batchDF, topK = 5)

          val changed = prevTop5 != currentTop5

          if (changed) {
            // Convert top5 to DataFrame
            import batchDF.sparkSession.implicits._
            val top5DF = currentTop5.toDF("user", "approx_count")

            // Save to Mongo
            top5DF.write
              .format("mongo")
              .option("uri", uri)
              .option("database", database)
              .option("collection", collection)
              .mode("overwrite")
              .save()

            prevTop5 = currentTop5
          }
        }
      }
      .start()

    cmsSave
  }
}
