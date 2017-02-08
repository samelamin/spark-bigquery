package com.samelamin.spark.bigquery

import java.io.File

import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import com.google.cloud.hadoop.io.bigquery._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.samelamin.spark.bigquery.converters.BigQueryAdapter
import org.apache.commons.io.FileUtils
import org.apache.spark.sql._
import org.mockito.Matchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.FeatureSpec
import org.scalatest.mock.MockitoSugar
/**
  * Createdby sam elamin on 1/12/17.
  */
class BigQueryClientSpecs extends FeatureSpec with DataFrameSuiteBase with MockitoSugar {
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

    // Mock getting Bigquery jobs
    when(bigQueryMock.jobs().get(any[String], any[String]).execute())
      .thenReturn(jobHandle)
    when(bigQueryMock.jobs().insert(any[String], any[Job]).execute())
      .thenReturn(jobHandle)

    val bigQueryClient = new BigQueryClient(sqlCtx, bigQueryMock)
    bigQueryClient
  }

  scenario("When writing to BQ") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val gcsPath = "/tmp/testfile2.json"
    FileUtils.deleteQuietly(new File(gcsPath))
    val adaptedDf = BigQueryAdapter(sc.parallelize(List(1, 2, 3)).toDF)
    val bigQueryMock =  mock[Bigquery](RETURNS_DEEP_STUBS)
    val fullyQualifiedOutputTableId = "testProjectID:test_dataset.test"
    val targetTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId)
    val bigQueryClient = setupBigQueryClient(sqlCtx, bigQueryMock)
    val bigQuerySchema = BigQuerySchema(adaptedDf)

    bigQueryClient.load(targetTable,bigQuerySchema,gcsPath)
    verify(bigQueryMock.jobs().insert(mockitoEq(BQProjectId),any[Job]), times(1)).execute()
  }

  scenario("When reading from BQ") {
    val sqlCtx = sqlContext
    val fullyQualifiedOutputTableId = "testProjectID:test_dataset.test"
    val sqlQuery = s"select * from $fullyQualifiedOutputTableId"
    sqlContext.setBigQueryProjectId(BQProjectId)
    val bigQueryMock =  mock[Bigquery](RETURNS_DEEP_STUBS)
    val bigQueryClient = setupBigQueryClient(sqlCtx, bigQueryMock)
    bigQueryClient.selectQuery(sqlQuery)
    verify(bigQueryMock.jobs().insert(mockitoEq(BQProjectId),any[Job]), times(1)).execute()
  }
}
