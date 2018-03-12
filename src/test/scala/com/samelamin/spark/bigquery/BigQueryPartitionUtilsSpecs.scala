package com.samelamin.spark.bigquery

import java.io.File

import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import com.google.cloud.hadoop.io.bigquery._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.samelamin.spark.bigquery.converters.{BigQueryAdapter, SchemaConverters}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.mockito.Matchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.FeatureSpec
import org.scalatest.mock.MockitoSugar

/**
  * Createdby sam elamin on 1/12/17.
  */
class BigQueryPartitionUtilsSpecs extends FeatureSpec with DataFrameSuiteBase with MockitoSugar {
  val BQProjectId = "google.com:foo-project"

  def setupBigQueryClient(sqlCtx: SQLContext, bigQueryMock: Bigquery): BigQueryClient = {
    val fakeJobReference = new JobReference()
    fakeJobReference.setProjectId(BQProjectId)
    fakeJobReference.setJobId("bigquery-job-1234")
    val dataProjectId = "publicdata"
    // Create the job result.
    val jobStatus = new JobStatus()
    jobStatus.setState("DONE")
    jobStatus.setErrorResult(null)

    val jobHandle = new Job()
    jobHandle.setStatus(jobStatus)
    jobHandle.setJobReference(fakeJobReference)

    // Create table reference.
    val tableRef = new TableReference()
    tableRef.setProjectId(dataProjectId)
    tableRef.setDatasetId("test_dataset")
    tableRef.setTableId("test_table")

    val jsonFactory = new JacksonFactory()
    val exception = GoogleJsonResponseExceptionFactoryTesting.newMock(jsonFactory,404,"table not found")

    val table = new Table()
    // Mock getting Bigquery jobs
    when(bigQueryMock.jobs().get(any[String], any[String]).execute())
      .thenReturn(jobHandle)
    when(bigQueryMock.jobs().insert(any[String], any[Job]).execute())
      .thenReturn(jobHandle)
    when(bigQueryMock.tables().get(any[String], any[String], any[String]).execute())
      .thenThrow(exception)

    val bigQueryClient = new BigQueryClient(sqlCtx, bigQueryMock)
    bigQueryClient
  }

  scenario("When writing to BQ time partitioned") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val gcsPath = "/tmp/testfile2.json"
    FileUtils.deleteQuietly(new File(gcsPath))
    val adaptedDf = BigQueryAdapter(sc.parallelize(List(1, 2, 3)).toDF)
    val bigQueryMock =  mock[Bigquery](RETURNS_DEEP_STUBS)
    val fullyQualifiedOutputTableId = "testProjectID:testDataset.test"
    val targetTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val bigQueryClient = setupBigQueryClient(sqlCtx, bigQueryMock)
    val bigQuerySchema = SchemaConverters.SqlToBQSchema(adaptedDf)

    bigQueryClient.load(targetTable,bigQuerySchema,gcsPath,true)
    verify(bigQueryMock.jobs().insert(mockitoEq(BQProjectId),any[Job]), times(1)).execute()
  }

  scenario("When writing to a bq with table decorators") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val gcsPath = "/tmp/testfile2.json"
    FileUtils.deleteQuietly(new File(gcsPath))
    val adaptedDf = BigQueryAdapter(sc.parallelize(List(1, 2, 3)).toDF)
    val bqSchema = SchemaConverters.SqlToBQSchema(adaptedDf)
    val bqMock =  mock[Bigquery](RETURNS_DEEP_STUBS)

    val fullyQualifiedOutputTableId = "testProjectID:testDataset.test"
    val cleanTableName = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val table = new Table()
    table.setTableReference(cleanTableName)
    val timePartitioning = new TimePartitioning()
    table.setSchema(bqSchema)

    timePartitioning.setType("DAY")
    table.setTimePartitioning(timePartitioning)

    val targetTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val bigQueryClient = setupBigQueryClient(sqlCtx, bqMock)
    val bigQuerySchema = SchemaConverters.SqlToBQSchema(adaptedDf)

    bigQueryClient.load(targetTable,bigQuerySchema,gcsPath,true)

    verify(bqMock.jobs().insert(mockitoEq(BQProjectId),any[Job]), times(1)).execute()
    verify(bqMock.tables(), times(1)).insert("testProjectID","testDataset",table)
  }


  scenario("When writing to a bq with a ingestion-time partitioned column specified") {
    val sqlCtx = sqlContext
    sqlCtx.hadoopConf.set("time_partitioning_column","bq_load_timestamp")
    import sqlCtx.implicits._
    val gcsPath = "/tmp/testfile2.json"
    FileUtils.deleteQuietly(new File(gcsPath))
    val adaptedDf = BigQueryAdapter(sc.parallelize(List(1, 2, 3)).toDF)
    val bqSchema = SchemaConverters.SqlToBQSchema(adaptedDf)
    val bqMock =  mock[Bigquery](RETURNS_DEEP_STUBS)

    val fullyQualifiedOutputTableId = "testProjectID:testDataset.test"
    val cleanTableName = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val table = new Table()
    table.setTableReference(cleanTableName)
    val timePartitioning = new TimePartitioning()
    table.setSchema(bqSchema)

    timePartitioning.setField("bq_load_timestamp")
    timePartitioning.setType("DAY")
    table.setTimePartitioning(timePartitioning)

    val targetTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val bigQueryClient = setupBigQueryClient(sqlCtx, bqMock)
    val bigQuerySchema = SchemaConverters.SqlToBQSchema(adaptedDf)

    bigQueryClient.load(targetTable,bigQuerySchema,gcsPath,true)

    verify(bqMock.jobs().insert(mockitoEq(BQProjectId),any[Job]), times(1)).execute()
    verify(bqMock.tables(), times(1)).insert("testProjectID","testDataset",table)
  }
}
