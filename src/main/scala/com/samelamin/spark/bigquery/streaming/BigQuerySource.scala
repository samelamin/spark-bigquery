package com.samelamin.spark.bigquery.streaming

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.io.bigquery.{AbstractBigQueryInputFormat, BigQueryStrings}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType

/**
  * Created by samelamin on 29/01/2017.
  */
  class BigQuerySource(sqlContext: SQLContext, user_schema: Option[StructType],
                       options: Map[String, String]) extends Source {
  val hadoopConfiguration = sqlContext.sparkContext.hadoopConfiguration

  /** Returns the schema of the data from this source */
  def schema: StructType = {
    null
  }

  /** Returns the maximum available offset for this source. */
  def getOffset: Option[Offset] = {
    null
  }

  /**
    * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None` then
    * the batch should begin with the first available record. This method must always return the
    * same data for a particular `start` and `end` pair.
    */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val fullyQualifiedOutputTableId = options.get("tableSpec").get

    bigQueryTable(BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId))
   }

  def bigQueryTable(tableRef: TableReference): DataFrame = {

    hadoopConfiguration.setClass(
      AbstractBigQueryInputFormat.INPUT_FORMAT_CLASS_KEY,
      classOf[AvroBigQueryInputFormat], classOf[InputFormat[LongWritable, GenericData.Record]])

    BigQueryConfiguration.configureBigQueryInput(
      conf, tableRef.getProjectId, tableRef.getDatasetId, tableRef.getTableId)

    val fClass = classOf[AvroBigQueryInputFormat]
    val kClass = classOf[LongWritable]
    val vClass = classOf[GenericData.Record]
    val rdd = sc
      .newAPIHadoopRDD(conf, fClass, kClass, vClass)
      .map(_._2)
    val schemaString = rdd.map(_.getSchema.toString).first()
    val schema = new Schema.Parser().parse(schemaString)

    val structType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
    val converter = SchemaConverters.createConverterToSQL(schema)
      .asInstanceOf[GenericData.Record => Row]
    self.createDataFrame(rdd.map(converter), structType)
  }
}

