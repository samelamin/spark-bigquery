package com.samelamin.spark.bigquery.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.{FileStreamSinkLog, Sink}
import com.samelamin.spark._
import org.slf4j.LoggerFactory

import scala.util.Try
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging

/**
  * A simple Structured Streaming sink which writes the data frame to Google Bigquery.
  *
  * @param options options passed from the upper level down to the dataframe writer.
  */
class BigQuerySink(sparkSession: SparkSession, path: String, options: Map[String, String]) extends Sink {
  private val logger = LoggerFactory.getLogger(classOf[BigQuerySink])
  private val basePath = new Path(path)
  private val logPath = new Path(basePath, BigQuerySink.metadataDir)

  private val fileLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, logPath.toUri.toString)
  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logger.info(s"Skipping already committed batch $batchId")
    } else {
      val fullyQualifiedOutputTableId = options.get("tableReferenceSink").get
      val isPartitionByDay = Try(options.get("partitionByDay").get.toBoolean).getOrElse(true)
      data.saveAsBigQueryTable(fullyQualifiedOutputTableId, isPartitionByDay)
    }
  }
}

object BigQuerySink {
  // The name of the subdirectory that is used to store metadata about which files are valid.
  val metadataDir = "_spark_metadata"
}


