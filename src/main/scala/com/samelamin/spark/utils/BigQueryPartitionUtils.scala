package com.samelamin.spark.utils

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.{Table, TableReference, TimePartitioning}
import com.samelamin.spark.bigquery.BigQueryServiceFactory
import org.apache.log4j.LogManager
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

/**
  * Created by root on 1/9/17.
  */
object BigQueryPartitionUtils {
  private val logger = LogManager.getRootLogger()

  val DEFAULT_TABLE_EXPIRATION_MS = 259200000L
  val bqService = BigQueryServiceFactory.getService

  def createBigQueryPartitionedTable(targetTable: TableReference): Any = {
    val datasetId = targetTable.getDatasetId
    val projectId: String = targetTable.getProjectId
    val tableName = targetTable.getTableId
    try {
      logger.info("Creating Time Partitioned Table")
      val table = new Table()
      table.setTableReference(targetTable)
      val timePartitioning = new TimePartitioning()
      timePartitioning.setType("DAY")
      timePartitioning.setExpirationMs(DEFAULT_TABLE_EXPIRATION_MS)
      table.setTimePartitioning(timePartitioning)
      val request = bqService.tables().insert(projectId, datasetId, table)
      val response = request.execute()
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 409 =>
        logger.info(s"$projectId:$datasetId.$tableName already exists")
      case NonFatal(e) => throw e
    }
  }
}
