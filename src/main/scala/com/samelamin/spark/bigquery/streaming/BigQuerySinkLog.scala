package com.samelamin.spark.bigquery.streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Created by sam elamin on 08/02/2017.
  */
class BigQuerySinkLog(sparkSession: SparkSession, path: String) {
  private val logPath = new Path(path, "batch_logs.json").toString

  def getLatest(): Option[Long] = {
    try {
      import sparkSession.implicits._
      val df = sparkSession.read.json(logPath).as[Long]
      df.show()
      val latest: Long = df
        .sort(desc("inserted_batches"))
        .first()
      return Some(latest)
    } catch {
      case e: Exception => None
    }
  }

  def writeBatch(batchId: Long):Unit = {
    import sparkSession.implicits._
    val df = Seq(batchId).toDF("inserted_batches").as[Long]
    df.write.mode(SaveMode.Overwrite).json(logPath)
  }
}


