package com.samelamin
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import com.samelamin.spark.bigquery.{BigQueryClient, DataFrameWriterFunctions}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SQLContext}

/**
  * Created by root on 28/01/2017.
  */
package object spark {
  implicit def toDataFrameWriterFunctions(dfw: DataFrameWriter[Row]): DataFrameWriterFunctions =
    new DataFrameWriterFunctions(dfw)

  implicit class BigQuerySQLContext(sqlContext: SQLContext) extends Serializable {
    lazy val bq = BigQueryClient.getInstance(sqlContext)
    @transient
    lazy val hadoopConf = sqlContext.sparkContext.hadoopConfiguration

    /**
      * Set whether to allow schema updates
      */
    def setAllowSchemaUpdates(value: Boolean = true): Unit = {
      hadoopConf.set(bq.ALLOW_SCHEMA_UPDATES, value.toString)
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
      bq.query(sqlQuery)
      val tableData = sqlContext.sparkContext.newAPIHadoopRDD(
        hadoopConf,
        classOf[GsonBigQueryInputFormat],
        classOf[LongWritable],
        classOf[JsonObject]).map(_._2.toString)

      val df = sqlContext.read.json(tableData)
      df
    }
  }
}
