package com.samelamin.spark.bigquery.utils

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.model.{Table, TableReference, TimePartitioning}
import com.samelamin.spark.bigquery.BigQueryServiceFactory
import org.apache.log4j.LogManager
import scala.util.control.NonFatal

/**
  * Created by sam elamin on 1/9/17.
  */
object BigQueryPartitionUtils {
  private val logger = LogManager.getRootLogger()

  val DEFAULT_TABLE_EXPIRATION_MS = 259200000L
  val bqService = BigQueryServiceFactory.getService

  def createBigQueryPartitionedTable(targetTable: TableReference, timePartitionExpiration: Long = 0): Any = {
    val datasetId = targetTable.getDatasetId
    val projectId: String = targetTable.getProjectId
    val tableName = targetTable.getTableId
    try {
      logger.info("Creating Time Partitioned Table")
      val table = new Table()
      table.setTableReference(targetTable)
      val timePartitioning = new TimePartitioning()
      timePartitioning.setType("DAY")
      table.setTimePartitioning(timePartitioning)
      if(timePartitionExpiration > 0){
        table.setExpirationTime(timePartitionExpiration)
      }
      val request = bqService.tables().insert(projectId, datasetId, table)
      val response = request.execute()
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 409 =>
        logger.info(s"$projectId:$datasetId.$tableName already exists")
      case NonFatal(e) => throw e
    }
  }
}
