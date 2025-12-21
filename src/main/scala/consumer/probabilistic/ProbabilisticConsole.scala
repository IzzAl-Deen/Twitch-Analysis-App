package consumer.probabilistic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object ProbabilisticConsole {
  def Save(DF: DataFrame): StreamingQuery = {
    var prevTop5: Seq[(String, Long)] = Seq()
    val cmsSave = DF.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        println(s"\n BATCH $batchId")
        println(s"Rows in batch: ${batchDF.count()}")


        if (!batchDF.isEmpty) {
          val currentTop5 = CountMinSketch.Run(batchDF, topK = 5)

          val changed = prevTop5 != currentTop5

          if (changed) {
            println(s"\n TOP 5 USERS UPDATED (Batch $batchId) ")
            currentTop5.foreach { case (user, count) =>
              println(s"$user -> ~$count messages")
            }

            prevTop5 = currentTop5
          }
        }
      }
      .start()

    cmsSave
  }
}
