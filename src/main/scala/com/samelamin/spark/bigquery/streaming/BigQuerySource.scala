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
  override def schema: StructType = {
    currentSchema
  }
  var currentOffset: Long = 0l
  override def getOffset: Option[Offset] = {
    val last_modified_time = 10l

    logger.warn(s"current offset is $currentOffset")
    if(currentOffset == 0l) {
      currentOffset = last_modified_time
    }

    Some(LongOffset(currentOffset))

  }

  /**
    * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None` then
    * the batch should begin with the first available record. This method must always return the
    * same data for a particular `start` and `end` pair.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val fullyQualifiedOutputTableId = options.get("tableSpec").get
    logger.warn(s"******* table spec is $fullyQualifiedOutputTableId")
    logger.warn(s"fully qualified table name is $fullyQualifiedOutputTableId")
    val query = s"""SELECT
                  *
                  FROM
                  $fullyQualifiedOutputTableId"""
    currentOffset += 10l
    val df = sqlContext.bigQuerySelect(query)
    currentSchema = df.schema
    schema
    logger.warn("********** new schema set in get batch is ")
    logger.warn(currentSchema.toString())
    df
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