package com.samelamin.spark.bigquery
import java.math.BigInteger
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.samelamin.spark.utils.BigQueryPartitionUtils
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.api.services.bigquery.model._
import com.google.api.services.bigquery.model.{Dataset => BQDataset}
import com.google.cloud.hadoop.io.bigquery._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.gson.JsonParser
import com.samelamin.spark.{CreateDisposition, WriteDisposition}
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql._
import scala.collection.JavaConverters._
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import scala.util.Random
import scala.util.control.NonFatal

/**
  * Created by sam elamin on 11/01/2017.
  */
object BigQueryClient {
  val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
  private val SCOPES = List(BigqueryScopes.BIGQUERY).asJava
  private var instance: BigQueryClient = null

  def getInstance(sqlContext: SQLContext): BigQueryClient = {
    if (instance == null) {
      val bigquery = {
        val credential = GoogleCredential.getApplicationDefault.createScoped(SCOPES)
        new Bigquery.Builder(new NetHttpTransport, new JacksonFactory, credential)
          .setApplicationName("spark-bigquery")
          .build()
      }
      instance = new BigQueryClient(sqlContext,bigquery)
    }
    instance
  }
}

class BigQueryClient(sqlContext: SQLContext, var bigquery: Bigquery = null) extends Serializable  {
  @transient
  lazy val jsonParser = new JsonParser()
  @transient
  val hadoopConf = sqlContext.sparkContext.hadoopConfiguration

  val STAGING_DATASET_PREFIX = "bq.staging_dataset.prefix"
  val STAGING_DATASET_PREFIX_DEFAULT = "spark_bigquery_staging_"
  val STAGING_DATASET_LOCATION = "bq.staging_dataset.location"
  val STAGING_DATASET_LOCATION_DEFAULT = "US"
  val STAGING_DATASET_TABLE_EXPIRATION_MS = 86400000L
  val STAGING_DATASET_DESCRIPTION = "Spark BigQuery staging dataset"
  val DEFAULT_TABLE_EXPIRATION_MS = 259200000L
  val ALLOW_SCHEMA_UPDATES = "ALLOW_SCHEMA_UPDATES"
  val USE_STANDARD_SQL_DIALECT = "USE_STANDARD_SQL_DIALECT"
  private val logger = LoggerFactory.getLogger(classOf[BigQueryClient])
  private def projectId = hadoopConf.get(BigQueryConfiguration.PROJECT_ID_KEY)
  private def inConsole = Thread.currentThread().getStackTrace.exists(
    _.getClassName.startsWith("scala.tools.nsc.interpreter."))
  private val PRIORITY = if (inConsole) "INTERACTIVE" else "BATCH"
  private val TABLE_ID_PREFIX = "spark_bigquery"
  private val JOB_ID_PREFIX = "spark_bigquery"

  def load(destinationTable: TableReference,
           bigQuerySchema: String,
           gcsPath: String,
           isPartitionedByDay: Boolean = false,
           writeDisposition: WriteDisposition.Value = null,
           createDisposition: CreateDisposition.Value = null): Unit = {
    if(isPartitionedByDay) {
      BigQueryPartitionUtils.createBigQueryPartitionedTable(destinationTable)
    }
    val tableSchema = new TableSchema().setFields(BigQueryUtils.getSchemaFromString(bigQuerySchema))
    val allow_schema_updates = hadoopConf.get(ALLOW_SCHEMA_UPDATES,"false").toBoolean
    var loadConfig = new JobConfigurationLoad()
      .setSchema(tableSchema)
      .setDestinationTable(destinationTable)
      .setSourceFormat("NEWLINE_DELIMITED_JSON")
      .setSourceUris(List(gcsPath + "/*").asJava)

    if(allow_schema_updates) {
      loadConfig.setSchemaUpdateOptions(List("ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION").asJava)
    }
    if (writeDisposition != null) {
      loadConfig = loadConfig.setWriteDisposition(writeDisposition.toString)
    }
    if (createDisposition != null) {
      loadConfig = loadConfig.setCreateDisposition(createDisposition.toString)
    }

    val jobConfig = new JobConfiguration().setLoad(loadConfig)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute()
    waitForJob(job)
  }


  /**
    * Perform a BigQuery SELECT query and save results to a temporary table.
    */
  def selectQuery(sqlQuery: String): Unit = {
    val tableReference = queryCache.get(sqlQuery)
    val fullyQualifiedInputTableId = BigQueryStrings.toString(tableReference)
    BigQueryConfiguration.configureBigQueryInput(hadoopConf, fullyQualifiedInputTableId)
  }

  private val queryCache: LoadingCache[String, TableReference] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(STAGING_DATASET_TABLE_EXPIRATION_MS, TimeUnit.MILLISECONDS)
      .build[String, TableReference](new CacheLoader[String, TableReference] {
      override def load(key: String): TableReference = {
        val sqlQuery = key
        logger.info(s"Executing query $sqlQuery")
        val location = hadoopConf.get(STAGING_DATASET_LOCATION, STAGING_DATASET_LOCATION_DEFAULT)
        val destinationTable = temporaryTable(location)
        logger.info(s"Destination table: $destinationTable")
        val use_legacy_sql = !hadoopConf.get(USE_STANDARD_SQL_DIALECT,"true").toBoolean

        val job = createQueryJob(sqlQuery, destinationTable, dryRun = false,use_legacy_sql)

        waitForJob(job)
        destinationTable
      }
    })

  private def waitForJob(job: Job): Unit = {
    BigQueryUtils.waitForJobCompletion(bigquery, projectId, job.getJobReference, new Progressable {
      override def progress(): Unit = {}
    })
  }

  private def stagingDataset(location: String): DatasetReference = {
    // Create staging dataset if it does not already exist
    val prefix = hadoopConf.get(STAGING_DATASET_PREFIX, STAGING_DATASET_PREFIX_DEFAULT)
    val datasetId = prefix + location.toLowerCase
    try {
      bigquery.datasets().get(projectId, datasetId).execute()
      logger.info(s"Staging dataset $projectId:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 404 =>
        logger.info(s"Creating staging dataset $projectId:$datasetId")
        val dsRef = new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
        val ds = new BQDataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(STAGING_DATASET_TABLE_EXPIRATION_MS)
          .setDescription(STAGING_DATASET_DESCRIPTION)
          .setLocation(location)
        bigquery
          .datasets()
          .insert(projectId, ds)
          .execute()

      case NonFatal(e) => throw e
    }
    new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
  }

  private def temporaryTable(location: String): TableReference = {
    val now = Instant.now().toString(BigQueryClient.TIME_FORMATTER)
    val tableId = TABLE_ID_PREFIX + "_" + now + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(stagingDataset(location).getDatasetId)
      .setTableId(tableId)
  }

  private def createQueryJob(sqlQuery: String,
                             destinationTable: TableReference,
                             dryRun: Boolean,use_legacy_sql: Boolean = false): Job = {
    var queryConfig = new JobConfigurationQuery()
      .setQuery(sqlQuery)
      .setPriority(PRIORITY)
      .setCreateDisposition("CREATE_IF_NEEDED")
      .setWriteDisposition("WRITE_EMPTY")

    logger.info(s"Using legacy Sql: $use_legacy_sql")

    queryConfig.setUseLegacySql(use_legacy_sql)

    if (destinationTable != null) {
      queryConfig = queryConfig
        .setDestinationTable(destinationTable)
        .setAllowLargeResults(true)
    }

    val jobConfig = new JobConfiguration().setQuery(queryConfig).setDryRun(dryRun)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute()
  }

  private def createJobReference(projectId: String, jobIdPrefix: String): JobReference = {
    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    new JobReference().setProjectId(projectId).setJobId(fullJobId)
  }

  def getLatestModifiedTime(tableReference: TableReference): BigInteger = {
    val table = bigquery.tables().get(tableReference.getProjectId,tableReference.getDatasetId,tableReference.getTableId).execute()
    table.getLastModifiedTime()
  }

  def getTableSchema(tableReference: TableReference): TableSchema = {
    val table = bigquery.tables().get(tableReference.getProjectId,tableReference.getDatasetId,tableReference.getTableId).execute()
    table.getSchema()
  }

}