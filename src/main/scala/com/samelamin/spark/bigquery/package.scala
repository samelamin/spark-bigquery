package com.samelamin.spark

import java.math.BigInteger

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.io.bigquery._
import com.google.gson._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SQLContext}
import org.slf4j.LoggerFactory
import org.apache.avro.generic.GenericData
import com.google.cloud.hadoop.io.bigquery.AvroBigQueryInputFormat
import com.samelamin.spark.bigquery.converters.{BigQueryAdapter, SchemaConverters}
import org.apache.avro.Schema

import scala.util.Random
/**
  * Created by sam elamin on 28/01/2017.
  */
package object bigquery {
  implicit def toDataFrameWriterFunctions(dfw: DataFrameWriter[Row]): DataFrameWriterFunctions =
    new DataFrameWriterFunctions(dfw)

  implicit class BigQuerySQLContext(sqlContext: SQLContext) extends Serializable {
    lazy val bq = BigQueryClient.getInstance(sqlContext)
    @transient
    lazy val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    private val logger = LoggerFactory.getLogger(classOf[BigQueryClient])

    /**
      * Set whether to allow schema updates
      */
    def setAllowSchemaUpdates(value: Boolean = true): Unit = {
      hadoopConf.set(bq.ALLOW_SCHEMA_UPDATES, value.toString)
    }

    /**
      * Set whether to use the Standard SQL Dialect
      */
    def useStandardSQLDialect(value: Boolean = true): Unit = {
      hadoopConf.set(bq.USE_STANDARD_SQL_DIALECT, value.toString)
    }
    /**
      * Set GCP project ID for BigQuery.
      */
    def setBigQueryProjectId(projectId: String): Unit = {
      hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    }

    def setGSProjectId(projectId: String): Unit = {
      // Also set project ID for GCS connector
      hadoopConf.set("fs.gs.project.id", projectId)
    }

    /**
      * Set GCS bucket for temporary BigQuery files.
      */
    def setBigQueryGcsBucket(gcsBucket: String): Unit =
      hadoopConf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)

    /**
      * Set BigQuery dataset location, e.g. US, EU.
      */
    def setBigQueryDatasetLocation(location: String): Unit = {
      hadoopConf.set(bq.STAGING_DATASET_LOCATION, location)
    }

    /**
      * Set GCP JSON key file.
      */
    def setGcpJsonKeyFile(jsonKeyFile: String): Unit = {
      hadoopConf.set("mapred.bq.auth.service.account.json.keyfile", jsonKeyFile)
      hadoopConf.set("fs.gs.auth.service.account.json.keyfile", jsonKeyFile)
    }

    /**
      * Set GCP pk12 key file.
      */
    def setGcpPk12KeyFile(pk12KeyFile: String): Unit = {
      hadoopConf.set("google.cloud.auth.service.account.keyfile", pk12KeyFile)
      hadoopConf.set("mapred.bq.auth.service.account.keyfile", pk12KeyFile)
      hadoopConf.set("fs.gs.auth.service.account.keyfile", pk12KeyFile)
    }

    def bigQuerySelect(sqlQuery: String): DataFrame = {
      bq.selectQuery(sqlQuery)
      val tableData = sqlContext.sparkContext.newAPIHadoopRDD(
        hadoopConf,
        classOf[AvroBigQueryInputFormat],
        classOf[LongWritable],
        classOf[GenericData.Record]).map(x=>x._2)
      val schemaString = tableData.map(_.getSchema.toString).first()
      val schema = new Schema.Parser().parse(schemaString)
      val structType = SchemaConverters.avroToSqlType(schema).dataType.asInstanceOf[StructType]
      val converter = SchemaConverters.createConverterToSQL(schema)
        .asInstanceOf[GenericData.Record => Row]
      sqlContext.createDataFrame(tableData.map(converter), structType)
    }

    def getLatestBQModifiedTime(tableReference: String): Option[BigInteger] = {
      bq.getLatestModifiedTime(BigQueryStrings.parseTableReference(tableReference))
    }

    def getBigQuerySchema(tableReference: String): StructType = {
      SchemaConverters.BQToSQLSchema(bq.getTableSchema(BigQueryStrings.parseTableReference(tableReference)))
    }
  }
  implicit class BigQueryDataFrame(self: DataFrame) extends Serializable {
    val adaptedDf = BigQueryAdapter(self)
    private val logger = LoggerFactory.getLogger(classOf[BigQueryClient])

    @transient
    lazy val hadoopConf = self.sqlContext.sparkContext.hadoopConfiguration
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
                            writeDisposition: WriteDisposition.Value = null,
                            createDisposition: CreateDisposition.Value = null): Unit = {

      val destinationTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
      val bigQuerySchema = SchemaConverters.SqlToBQSchema(adaptedDf)
      val gcsPath = writeDFToGoogleStorage(adaptedDf,destinationTable,bigQuerySchema)
      bq.load(destinationTable,
        bigQuerySchema,
        gcsPath,
        isPartitionedByDay,
        writeDisposition,
        createDisposition)
      delete(new Path(gcsPath))
    }

    def writeDFToGoogleStorage(adaptedDf: DataFrame,
                               destinationTable: TableReference,
                               bigQuerySchema: String): String = {
      val tableName = BigQueryStrings.toString(destinationTable)

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

    private def delete(path: Path): Unit = {
      val fs = FileSystem.get(path.toUri, hadoopConf)
      fs.delete(path, true)
    }
  }

  object CreateDisposition extends Enumeration {
    val CREATE_IF_NEEDED, CREATE_NEVER = Value
  }

  object WriteDisposition extends Enumeration {
    val WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY = Value
  }
}
