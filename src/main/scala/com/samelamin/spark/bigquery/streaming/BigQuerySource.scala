package com.samelamin.spark.bigquery.streaming

import java.math.BigInteger
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.samelamin.spark.bigquery.BigQueryClient
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, _}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import com.samelamin.spark.bigquery._
import com.samelamin.spark.bigquery.converters.SchemaConverters
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
  * Created by sam elamin on 29/01/2017.
  */
  class BigQuerySource(sqlContext: SQLContext, user_schema: Option[StructType],
                       options: Map[String, String]) extends Source {
  val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  private val logger = LoggerFactory.getLogger(classOf[BigQuerySource])
  val fullyQualifiedOutputTableId = options.get("tableReferenceSource").get
  val timestampColumn = sqlContext.hadoopConf.get("timestamp_column","bq_load_timestamp")
  /** Returns the schema of the data from this source */
  override def schema: StructType = {
    BigQuerySource.DEFAULT_SCHEMA
  }

  override def getOffset: Option[Offset] = {
    val lastModified = sqlContext.getLatestBQModifiedTime(fullyQualifiedOutputTableId).getOrElse(BigInteger.ZERO)
    logger.info(s"$fullyQualifiedOutputTableId was last updated on ${lastModified.longValue()}")
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
    val startPartitionTime = new DateTime(startIndex).toLocalDate
    val endPartitionTime = new DateTime(endIndex).toLocalDate.toString
    logger.info(s"Fetching data between $startIndex and $endIndex")
    val query =
      s"""
         |SELECT
         |  *
         |FROM
         |  `${fullyQualifiedOutputTableId.replace(':','.')}`
         |WHERE
         |  $timestampColumn BETWEEN TIMESTAMP_MILLIS($startIndex) AND TIMESTAMP_MILLIS($endIndex)
         |  AND _PARTITIONTIME BETWEEN TIMESTAMP('$startPartitionTime') AND TIMESTAMP('$endPartitionTime')
         |  """.stripMargin

    val df = sqlContext.bigQuerySelect(query)
    df
  }

  override def stop(): Unit = {}
  def getConvertedSchema(sqlContext: SQLContext): StructType = {
    val bigqueryClient = BigQueryClient.getInstance(sqlContext)
    val tableReference = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    SchemaConverters.BQToSQLSchema(bigqueryClient.getTableSchema(tableReference))
  }
}

object BigQuerySource {
  val DEFAULT_SCHEMA = StructType(
    StructField("Sample Column", StringType) ::
      StructField("value", BinaryType) :: Nil
  )
}