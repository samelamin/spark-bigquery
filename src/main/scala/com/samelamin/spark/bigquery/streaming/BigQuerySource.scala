package com.samelamin.spark.bigquery.streaming

import java.util.concurrent.atomic.AtomicReference

import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.samelamin.spark.bigquery.{BigQueryClient, DefaultSource}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import com.samelamin.spark._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
/**
  * Created by samelamin on 29/01/2017.
  */
  class BigQuerySource(sqlContext: SQLContext, user_schema: Option[StructType],
                       options: Map[String, String]) extends Source {
  val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  private val logger = LoggerFactory.getLogger(classOf[BigQuerySource])
  var currentSchema:StructType = null
  /** Returns the schema of the data from this source */
  def schema: StructType = {
    currentSchema
  }
  var currentOffset: Long = 0l
  override def getOffset: Option[Offset] = {
    val last_modified_time = 1485727760120l
    currentOffset = last_modified_time
    logger.warn("current offset is 0")
    if(currentOffset == 0l) {
      None
    } else {
      Some(LongOffset(currentOffset))
    }
  }

  /**
    * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None` then
    * the batch should begin with the first available record. This method must always return the
    * same data for a particular `start` and `end` pair.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val fullyQualifiedOutputTableId = options.get("tableSpec").get
//    logger.warn(s"fully qualified table name is $fullyQualifiedOutputTableId")
//      val query = """SELECT
//                  last_modified_time AS last_modified,
//                  FROM
//                  [je-bi-datalake.test.test$__PARTITIONS_SUMMARY__]"""
//
//    val df = sqlContext.bigQuerySelect(query)
//    currentSchema = df.schema
//
//    df
    val startIndex = start.getOrElse(LongOffset(0L)).asInstanceOf[LongOffset].offset.toInt
    val endIndex = currentOffset
    val data: ArrayBuffer[(String, Long)] = ArrayBuffer.empty
    // Move consumed messages to persistent store.
    (startIndex + 1 to 100).foreach { id =>
      val element: (String, Long) = ("blag",currentOffset)
      data += element

    }
    logger.trace(s"Get Batch invoked, ${data.mkString}")
    import sqlContext.implicits._
    data.toDF("value", "timestamp")
   }

  override def stop(): Unit = {

  }
}

object BigQuerySource {
  val DEFAULT_SCHEMA = StructType(
    StructField(DefaultSource.DEFAULT_DOCUMENT_ID_FIELD, StringType) ::
      StructField("value", BinaryType) :: Nil
  )
}