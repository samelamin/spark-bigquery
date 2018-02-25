package com.samelamin.spark.bigquery.utils

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.{Table, TableReference, TableSchema, TimePartitioning}
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import org.apache.log4j.LogManager

import scala.util.control.NonFatal

/**
  * Created by sam elamin on 1/9/17.
  */
class BigQueryPartitionUtils(bqService: Bigquery)  {
  private val logger = LogManager.getRootLogger()
  val DEFAULT_TABLE_EXPIRATION_MS = 259200000L

  def createBigQueryPartitionedTable(targetTable: TableReference,
                                     timePartitionExpiration: Long = 0,
                                     tableSchema: TableSchema = null,
                                     timePartitioningField:String = null): Any = {
    val fullyQualifiedOutputTableId = BigQueryStrings.toString(targetTable)
    val decoratorsRegex = ".+?(?=\\$)".r
    val cleanTableName = BigQueryStrings
    .parseTableReference(decoratorsRegex.findFirstIn(fullyQualifiedOutputTableId)
    .getOrElse(fullyQualifiedOutputTableId))
    val projectId = cleanTableName.getProjectId
    val datasetId = cleanTableName.getDatasetId
    val tableId = cleanTableName.getTableId
    if(doesTableAlreadyExist(projectId,datasetId,tableId)) {
      return
    } else {
      logger.info(s"Creating Table $tableId")
      val table = new Table()
      table.setTableReference(cleanTableName)
      val timePartitioning = new TimePartitioning()
      timePartitioning.setType("DAY")
      // below is for the new bq partitoning feture
      if(timePartitioningField != null) {
        timePartitioning.setField(timePartitioningField)
      }
      table.setTimePartitioning(timePartitioning)
      if (timePartitionExpiration > 0) {
        table.setExpirationTime(timePartitionExpiration)
      }
      table.setSchema(tableSchema)
      bqService.tables().insert(cleanTableName.getProjectId, cleanTableName.getDatasetId, table).execute()
      logger.info(s"Table $tableId created")
    }

  }

  def doesTableAlreadyExist(projectId: String, datasetId: String, tableId: String): Boolean = {
    try {
      bqService.tables().get(projectId,datasetId,tableId).execute()
      return true
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 404 =>
        logger.info(s"$projectId:$datasetId.$tableId does not exist")
        return false
      case NonFatal(e) => throw e
    }
  }
}
