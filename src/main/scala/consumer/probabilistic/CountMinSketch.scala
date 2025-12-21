package consumer.probabilistic

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object CountMinSketch {

  def Run(df: DataFrame, topK: Int): Seq[(String, Long)] = {

    val cms = df.stat.countMinSketch(
      "user",
      eps = 0.01,
      confidence = 0.99,
      seed = 42
    )

    val candidates = mutable.Map[String, Long]()

    df.select("user")
      .distinct()
      .collect()
      .foreach { row =>
        val user = row.getString(0)
        val estimate = cms.estimateCount(user)
        candidates.update(user, estimate)
      }

    candidates.toSeq
      .sortBy(-_._2)
      .take(topK)
  }
}
