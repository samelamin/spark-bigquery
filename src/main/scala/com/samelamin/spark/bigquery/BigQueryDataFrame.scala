package com.samelamin.spark.bigquery

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.io.bigquery._
import com.google.gson._
import com.samelamin.spark.bigquery.converters.{BigQueryAdapter, SchemaConverters}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import scala.util.Random
/**
  * Created by samelamin on 12/08/2017.
  */
class BigQueryDataFrame(self: DataFrame) extends Serializable {
  val adaptedDf = BigQueryAdapter(self)
  private val logger = LoggerFactory.getLogger(classOf[BigQueryClient])

  @transient
  lazy val hadoopConf = self.sparkSession.sparkContext.hadoopConfiguration
  lazy val bq = BigQueryClient.getInstance(self.sqlContext)

  @transient
  lazy val jsonParser = new JsonParser()

  /**
    * Save DataFrame data into BigQuery table using Hadoop writer API
    *
    * @param fullyQualifiedOutputTableId output-table id of the form
    *                                    [optional projectId]:[datasetId].[tableId]
    * @param isPartitionedByDay partion the table by day
    */
  def saveAsBigQueryTable(fullyQualifiedOutputTableId: String,
                          isPartitionedByDay: Boolean = false,
                          timePartitionExpiration: Long = 0,
                          writeDisposition: WriteDisposition.Value = null,
                          createDisposition: CreateDisposition.Value = null): Unit = {
    val destinationTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val bigQuerySchema = SchemaConverters.SqlToBQSchema(adaptedDf)
    val gcsPath = writeDFToGoogleStorage(adaptedDf,destinationTable,bigQuerySchema)
    bq.load(destinationTable,
      bigQuerySchema,
      gcsPath,
      isPartitionedByDay,
      timePartitionExpiration,
      writeDisposition,
      createDisposition)
    delete(new Path(gcsPath))
  }


  def saveAsBigQueryTable2(fullyQualifiedOutputTableId: String): Unit = {
  saveAsBigQueryTable(fullyQualifiedOutputTableId,false)
  }

  def writeDFToGoogleStorage(adaptedDf: DataFrame,
                             destinationTable: TableReference,
                             bigQuerySchema: String): String = {
    val tableName = BigQueryStrings.toString(destinationTable)

    BigQueryConfiguration.configureBigQueryOutput(hadoopConf, tableName, bigQuerySchema)
    hadoopConf.set("mapreduce.job.outputformat.class", classOf[BigQueryOutputFormat[_, _]].getName)
    val bucket = self.sparkSession.conf.get(BigQueryConfiguration.GCS_BUCKET_KEY)
    val temp = s"spark-bigquery-${System.currentTimeMillis()}=${Random.nextInt(Int.MaxValue)}"
    val gcsPath = s"gs://$bucket/hadoop/tmp/spark-bigquery/$temp"
    if(hadoopConf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY) == null) {
      hadoopConf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, gcsPath)
    }

    logger.info(s"Loading $gcsPath into $tableName")
    adaptedDf
      .toJSON
      .rdd
      .map(json => (null, jsonParser.parse(json)))
      .saveAsNewAPIHadoopFile(gcsPath,
        classOf[GsonBigQueryInputFormat],
        classOf[LongWritable],
        classOf[TextOutputFormat[NullWritable, JsonObject]],
        hadoopConf)
    gcsPath
  }



  private def delete(path: Path): Unit = {
    val fs = FileSystem.get(path.toUri, hadoopConf)
    fs.delete(path, true)
  }
}
