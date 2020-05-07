spark-bigquery
===============

[![Build Status](https://travis-ci.org/samelamin/spark-bigquery.png)](https://travis-ci.org/samelamin/spark-bigquery)

This Spark module allows saving DataFrame as BigQuery table.

The project was inspired by [spotify/spark-bigquery](https://github.com/spotify/spark-bigquery), but there are several differences and enhancements:

* Use of the Structured Streaming API

* Use within Pyspark

* Saving via Decorators

* Allow saving to partitioned tables

* Easy integration with [Databricks](https://github.com/samelamin/spark-bigquery/blob/master/Databricks.md)

* Use of Standard SQL

* Use Of Time-Ingested Partition Columns

* Run Data Manipulation Language Queries [DML](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-manipulation-language)

* Update schemas on writes using the [setSchemaUpdateOptions](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/JobConfigurationQuery.html#setSchemaUpdateOptions(java.util.List))

* JSON is used as an intermediate format instead of Avro. This allows having fields on different levels named the same:

```json
{
  "obj": {
    "data": {
      "data": {}
    }
  }
}
```
* DataFrame's schema is automatically adapted to a legal one:

  1. Illegal characters are replaced with `_`
  2. Field names are converted to lower case to avoid ambiguity
  3. Duplicate field names are given a numeric suffix (`_1`, `_2`, etc.)


### Docker! 
I created a container that launches zepplin with spark and the connector for ease of use and quick startup. You can find it [here](https://github.com/samelamin/docker-zeppelin)

## Usage

### Including spark-bigquery into your project

#### Maven

```xml
<repositories>
  <repository>
    <id>oss-sonatype</id>
    <name>oss-sonatype</name>
    <url>https://oss.sonatype.org/content/repositories/releases/</url>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.github.samelamin</groupId>
    <artifactId>spark-bigquery_${scala.binary.version}</artifactId>
    <version>0.2.6</version>
  </dependency>
</dependencies>
```

#### SBT

To use it in a local SBT console first add the package as a dependency then set up your project details
```sbt
resolvers += Opts.resolver.sonatypeReleases

libraryDependencies += "com.github.samelamin" %% "spark-bigquery" % "0.2.6"
```

```scala
import com.samelamin.spark.bigquery._

// Set up GCP credentials
sqlContext.setGcpJsonKeyFile("<JSON_KEY_FILE>")

// Set up BigQuery project and bucket
sqlContext.setBigQueryProjectId("<BILLING_PROJECT>")
sqlContext.setBigQueryGcsBucket("<GCS_BUCKET>")

// Set up BigQuery dataset location, default is US
sqlContext.setBigQueryDatasetLocation("<DATASET_LOCATION>")
```

### Structured Streaming from S3/HDFS to BigQuery

S3,Blob Storage or HDFS are the defacto technology for storage in the cloud, this package allows you to stream any data added to a Big Query Table of your choice
```scala
import com.samelamin.spark.bigquery._

val df = spark.readStream.json("s3a://bucket")

df.writeStream
      .option("checkpointLocation", "s3a://checkpoint/dir")
      .option("tableReferenceSink","my-project:my_dataset.my_table")
      .format("com.samelamin.spark.bigquery")
      .start()
```

### Structured Streaming from BigQuery Table

You can use this connector to stream from a BigQuery Table. The connector uses a Timestamped column to get offsets. 

```scala
import com.samelamin.spark.bigquery._

val df = spark
          .readStream
          .option("tableReferenceSource","my-project:my_dataset.my_table")
          .format("com.samelamin.spark.bigquery")
          .load()
```
You can also specify a custom timestamp column: 
```scala
import com.samelamin.spark.bigquery._

sqlContext.setBQTableTimestampColumn("column_name")
```


You can also specify a custom Time Ingested Partition column: 
```scala
import com.samelamin.spark.bigquery._

sqlContext.setBQTimePartitioningField("column_name")
```

### Saving DataFrame using BigQuery Hadoop writer API
By Default any table created by this connector has a timestamp column of `bq_load_timestamp` which has the value of the current timestamp.
```scala
import com.samelamin.spark.bigquery._

val df = ...
df.saveAsBigQueryTable("project-id:dataset-id.table-name")
```

You can also save to a table decorator by saving to `dataset-id.table-name$YYYYMMDD`


### Saving DataFrame using Pyspark

```python
from pyspark.sql import SparkSession

BQ_PROJECT_ID = "projectId"
DATASET_ID = "datasetId"
TABLE_NAME = "tableName"

KEY_FILE = "/path/to/service_account.json" # When not on GCP
STAGING_BUCKET = "gcs-bucket"              # Intermediate JSON files
DATASET_LOCATION = "US"                    # Location for dataset creation

# Start session and reference the JVM package via py4j for convienence
session = SparkSession.builder.getOrCreate()
bigquery = session._sc._jvm.com.samelamin.spark.bigquery

# Prepare the bigquery context
bq = bigquery.BigQuerySQLContext(session._wrapped._jsqlContext)
bq.setGcpJsonKeyFile(KEY_FILE)
bq.setBigQueryProjectId(BQ_PROJECT_ID)
bq.setGSProjectId(BQ_PROJECT_ID)
bq.setBigQueryGcsBucket(STAGING_BUCKET)
bq.setBigQueryDatasetLocation(DATASET_LOCATION)

# Extract and Transform a dataframe
# df = session.read.csv(...)

# Load into a table or table partition
bqDF = bigquery.BigQueryDataFrame(df._jdf)
bqDF.saveAsBigQueryTable(
    "{0}:{1}.{2}".format(BQ_PROJECT_ID, DATASET_ID, TABLE_NAME),
    False, # Day paritioned when created
    0,     # Partition expired when created
    bigquery.__getattr__("package$WriteDisposition$").__getattr__("MODULE$").WRITE_EMPTY(),
    bigquery.__getattr__("package$CreateDisposition$").__getattr__("MODULE$").CREATE_IF_NEEDED(),
)
```

Submit with:

```bash
pyspark yourjob.py --packages com.github.samelamin:spark-bigquery_2.11:0.2.6
```

Or

```bash
gcloud dataproc jobs submit pyspark yourjob.py --properties spark.jars.packages=com.github.samelamin:spark-bigquery_2.11:0.2.6
```

### Reading DataFrame From BigQuery

```scala
import com.samelamin.spark.bigquery._
val sqlContext = spark.sqlContext

sqlContext.setBigQueryGcsBucket("bucketname")
sqlContext.setBigQueryProjectId("projectid")
sqlContext.setGcpJsonKeyFile("keyfilepath")
sqlContext.hadoopConf.set("fs.gs.project.id","projectid")

val df = spark.sqlContext.read.format("com.samelamin.spark.bigquery").option("tableReferenceSource","bigquery-public-data:samples.shakespeare").load()
``

### Reading DataFrame From BigQuery in Pyspark

```python
bq = spark._sc._jvm.com.samelamin.spark.bigquery.BigQuerySQLContext(spark._wrapped._jsqlContext)
df= DataFrame(bq.bigQuerySelect("SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare]"), session._wrapped)
```

### Running DML Queries

```scala
import com.samelamin.spark.bigquery._

// Load results from a SQL query
sqlContext.runDMLQuery("UPDATE dataset-id.table-name SET test_col = new_value WHERE test_col = old_value")
```
Please note that DML queries need to be done using Standard SQL

### Update Schemas

You can also allow the saving of a dataframe to update a schema:

```scala
import com.samelamin.spark.bigquery._

sqlContext.setAllowSchemaUpdates()
```

Notes on using this API:

 * Structured Streaming needs a partitioned table which is created by default when writing a stream
 * Structured Streaming needs a timestamp column where offsets are retrieved from, by default all tables are created with a `bq_load_timestamp` column with a default value of the current timstamp.
 * For use with Databricks please follow this [guide](https://github.com/samelamin/spark-bigquery/blob/master/Databricks.md)

# License

Copyright 2016 samelamin.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
