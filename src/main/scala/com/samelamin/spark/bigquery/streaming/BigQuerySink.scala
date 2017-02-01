package com.samelamin.spark.bigquery.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import com.samelamin.spark._
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * A simple Structured Streaming sink which writes the data frame to Google Bigquery.
  *
  * @param options options passed from the upper level down to the dataframe writer.
  */
class BigQuerySink(options: Map[String, String]) extends Sink with Serializable {
  private val logger = LoggerFactory.getLogger(classOf[BigQuerySink])

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val fullyQualifiedOutputTableId = options.get("tableReference").get
    val isPartitionByDay = Try(options.get("partitionByDay").get.toBoolean).getOrElse(true)

    logger.warn("************ saving schema is set to")
    logger.warn(data.schema.toString())
    data.saveAsBigQueryTable(fullyQualifiedOutputTableId,isPartitionByDay)
  }
}


