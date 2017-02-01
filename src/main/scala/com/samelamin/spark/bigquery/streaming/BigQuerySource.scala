package com.samelamin.spark.bigquery.streaming

import java.math.BigInteger
import java.util.concurrent.atomic.AtomicReference

import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.samelamin.spark.bigquery.{BigQueryClient, DefaultSource}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, _}
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
  var currentSchema:StructType = BigQuerySource.DEFAULT_SCHEMA
  val fullyQualifiedOutputTableId = options.get("tableReference").get

  /** Returns the schema of the data from this source */
  override def schema: StructType = {
    currentSchema
  }

  var currentOffset = 0l
  override def getOffset: Option[Offset] = {

    val lastModified: BigInteger = sqlContext.getLatestBQModifiedTime(fullyQualifiedOutputTableId)
    logger.warn(s"$fullyQualifiedOutputTableId was last updated on ${lastModified.longValue()}")
    Some(LongOffset(lastModified.longValue()))
  }

  /**
    * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None` then
    * the batch should begin with the first available record. This method must always return the
    * same data for a particular `start` and `end` pair.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startIndex = start.getOrElse(LongOffset(0L)).asInstanceOf[LongOffset].offset.toLong
    val endIndex = end.asInstanceOf[LongOffset].offset.toLong

    logger.warn(s"************* getting data between $startIndex and $endIndex")
    val query =
      s"""
         |SELECT
         |  *
         |FROM
         |  `${fullyQualifiedOutputTableId.replace(':','.')}`
         |WHERE
         |  _PARTITION_LOAD_TIME BETWEEN TIMESTAMP_MILLIS($startIndex) AND TIMESTAMP_MILLIS($endIndex)
         |""".stripMargin

    logger.warn(query)
    val df = sqlContext.bigQuerySelect(query)
    df
  }

  override def stop(): Unit = {

  }
}

object BigQuerySource {
  val DEFAULT_SCHEMA = StructType(
    StructField("Test Column", StringType) ::
      StructField("value", BinaryType) :: Nil
  )
}