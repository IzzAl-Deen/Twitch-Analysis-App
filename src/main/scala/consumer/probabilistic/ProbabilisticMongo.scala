package consumer.probabilistic

import config.MongoConfig._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import scala.collection.mutable

object ProbabilisticMongo {
  val cms = mutable.Map[String, Long]()

  def Save(df: DataFrame, collection: String): StreamingQuery = {
    var prevTop5: Seq[(String, Long)] = Seq()

    df.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        if (!batchDF.isEmpty) {

          val batchCounts = batchDF.groupBy("user").count().collect()
          batchCounts.foreach { row =>
            val user = row.getString(0)
            val count = row.getLong(1)
            cms.update(user, cms.getOrElse(user, 0L) + count)
          }

          val top5 = cms.toSeq.sortBy(-_._2).take(5)

          if (prevTop5 != top5) {
            val spark = batchDF.sparkSession
            import spark.implicits._

            val top5DF = top5.toDF("user", "approx_count")
              .withColumn("_id", col("user"))
              .withColumn("updated_at", current_timestamp())

            top5DF.write
              .format("mongo")
              .option("uri", uri)
              .option("database", database)
              .option("collection", collection)
              .option("replaceDocument", "true")
              .mode("append")
              .save()

            prevTop5 = top5
          }
        }
      }
      .start()
  }
}
