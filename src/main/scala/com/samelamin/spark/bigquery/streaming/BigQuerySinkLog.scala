package com.samelamin.spark.bigquery.streaming

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Created by sam elamin on 08/02/2017.
  */
class BigQuerySinkLog(sparkSession: SparkSession, path: String) {

  def getLatest(): Option[Long] = {
    import sparkSession.implicits._
    val df = sparkSession.read.json(path).as[Long]
    val latest = df
      .sort(desc("batchId"))
      .first()
    return Some(latest)
  }

  def writeBatch(batchId: Long):Unit = {
    import sparkSession.implicits._
    val df = Seq(batchId).toDF("inserted batches").as[Long]
    df.write.mode(SaveMode.Append).save(path)
  }
}


