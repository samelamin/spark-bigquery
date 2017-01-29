package com.samelamin.spark.bigquery.streaming

import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.samelamin.spark.bigquery.{BigQueryClient, DefaultSource}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{CompositeOffset, Offset, Source}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import com.samelamin.spark._
import org.slf4j.LoggerFactory
/**
  * Created by samelamin on 29/01/2017.
  */
  class BigQuerySource(sqlContext: SQLContext, user_schema: Option[StructType],
                       options: Map[String, String]) extends Source {
  val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration
  private val logger = LoggerFactory.getLogger(classOf[BigQuerySource])

  /** Returns the schema of the data from this source */
  def schema: StructType = {
    null
  }

  /** Returns the maximum available o ffset for this source. */
  override def getOffset: Option[PartitionOffset] = {
    val last_modified_time = 1485727760120l
    Some(new PartitionOffset(0,last_modified_time))
  }

  /**
    * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None` then
    * the batch should begin with the first available record. This method must always return the
    * same data for a particular `start` and `end` pair.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val fullyQualifiedOutputTableId = options.get("tableSpec").get
    logger.warn(s"fully qualified table name is $fullyQualifiedOutputTableId")
    logger.warn(s"offset is $end")

    val test = end.asInstanceOf[PartitionOffset]
    logger.warn(s"offset mapped is is $test")


    val last_modified_date = test.seqno

    logger.warn(s"last modified date is $last_modified_date")


      val query = s"""SELECT
                        customerid
                      FROM
                        [je-bi-datalake:test.test_2]
                      WHERE
                        _PARTITIONTIME BETWEEN 0
                        AND DATE_ADD(current_timestamp(), -1, "MINUTE");"""

    val df = sqlContext.bigQuerySelect(query)
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

class PartitionOffset(val vbid: Short, val seqno: Long) extends Offset {
  override def compareTo(other: Offset): Int = other match {
    case l: PartitionOffset => seqno.compareTo(l.seqno)
    case _ =>
      throw new IllegalArgumentException(s"Invalid comparison of $getClass with ${other.getClass}")
  }
}
