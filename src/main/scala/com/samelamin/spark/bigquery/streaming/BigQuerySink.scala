package com.samelamin.spark.bigquery.streaming

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryOutputFormat, BigQueryStrings, GsonBigQueryInputFormat}
import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import com.samelamin.spark._
import com.samelamin.spark.bigquery.{BigQueryAdapter, BigQueryClient, BigQuerySchema, WriteDisposition}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * A simple Structured Streaming sink which writes the data frame to the bucket.
  *
  * Note that for now Upsert is always used internally to not run into document already
  * exists exception, especially when writing aggregates.
  *
  * @param options options passed from the upper level down to the dataframe writer.
  */
class BigQuerySink(options: Map[String, String]) extends Sink with Serializable {
  private val logger = LoggerFactory.getLogger(classOf[BigQuerySink])
  @transient
  lazy val jsonParser = new JsonParser()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    val adaptedDf = BigQueryAdapter(data)
    @transient
    lazy val hadoopConf = data.sqlContext.sparkContext.hadoopConfiguration
    lazy val bq = BigQueryClient.getInstance(data.sqlContext)
    val fullyQualifiedOutputTableId = options.get("tableSpec").get

    logger.warn(s"fully qualified table name is $fullyQualifiedOutputTableId")

    val destinationTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val bigQuerySchema = BigQuerySchema(adaptedDf)
    val gcsPath = writeDFToGoogleStorage(adaptedDf,destinationTable,bigQuerySchema)
    bq.load(destinationTable,
      bigQuerySchema,
      gcsPath)
    delete(new Path(gcsPath),data.sqlContext.hadoopConf)
  }

  def writeDFToGoogleStorage(adaptedDf: DataFrame,
                             destinationTable: TableReference,
                             bigQuerySchema: String): String = {
    val tableName = BigQueryStrings.toString(destinationTable)
    val hadoopConf = adaptedDf.sqlContext.hadoopConf

    BigQueryConfiguration.configureBigQueryOutput(hadoopConf, tableName, bigQuerySchema)
    hadoopConf.set("mapreduce.job.outputformat.class", classOf[BigQueryOutputFormat[_, _]].getName)
    val bucket = hadoopConf.get(BigQueryConfiguration.GCS_BUCKET_KEY)
    val temp = s"spark-bigquery-${System.currentTimeMillis()}=${Random.nextInt(Int.MaxValue)}"
    val gcsPath = s"gs://$bucket/hadoop/tmp/spark-bigquery/$temp"
    hadoopConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, gcsPath)
    logger.info(s"Loading $gcsPath into $tableName")
    adaptedDf
      .toJSON
      .rdd
      .map(json => (null, jsonParser.parse(json)))
      .saveAsNewAPIHadoopFile(hadoopConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY),
        classOf[GsonBigQueryInputFormat],
        classOf[LongWritable],
        classOf[TextOutputFormat[NullWritable, JsonObject]],
        hadoopConf)
    gcsPath
  }

  private def delete(path: Path, hadoopConf: Configuration): Unit = {
    val fs = FileSystem.get(path.toUri, hadoopConf)
    fs.delete(path, true)
  }

}


