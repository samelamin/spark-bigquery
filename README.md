spark-bigquery
===============

[![Build Status](https://travis-ci.org/samelamin/spark-bigquery.png)](https://travis-ci.org/samelamin/spark-bigquery)

This Spark module allows saving DataFrame as BigQuery table.

The project was inspired by [spotify/spark-bigquery](https://github.com/spotify/spark-bigquery), but there are several differences:

* Use of the Structured Streaming API on Spark 2.1

* Allow saving to partitioned tables

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
    <version>0.1.2</version>
  </dependency>
</dependencies>
```

#### SBT

To use it in a local SBT console first add the package as a dependency then set up your project details
```sbt
resolvers += Opts.resolver.sonatypeReleases

libraryDependencies += "com.github.samelamin" %% "spark-bigquery" % "0.1.2"
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

S3 and HDFS are the defacto technology for storage in the cloud, this package allows you to stream any data added to a Big Query Table of your choice
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

### Saving DataFrame using BigQuery Hadoop writer API
By Default any table created by this connector has a timestamp column of `bq_load_timestamp` which has the value of the current timestamp.
```scala
import com.samelamin.spark.bigquery._

val df = ...
df.saveAsBigQueryTable("project-id:dataset-id.table-name")
```

### Reading DataFrame From BigQuery

```scala
import com.samelamin.spark.bigquery._


// Load everything from a table
val table = sqlContext.bigQueryTable("bigquery-public-data:samples.shakespeare")

// Load results from a SQL query
// Only legacy SQL dialect is supported for now
val df = sqlContext.bigQuerySelect(
  "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare]")
```

### Update Schemas

You can also allow the saving of a dataframe to update a schema:

```scala
import com.samelamin.spark.bigquery._

sqlContext.setAllowSchemaUpdates()
```

Notes on using this API:

 * Target data set must already exist
 * Structured Streaming needs a partitioned table which is created by default when writing a stream
 * Structured Streaming needs a timestamp column where offsets are retrieved from, by default all tables are created with a `bq_load_timestamp` column with a default value of the current timstamp.
 * Structured Streaming currently does not support schema updates
 * For use with Databricks please follow this [guide](https://github.com/samelamin/spark-bigquery/blob/master/Databricks.md)
# License

Copyright 2016 samelamin.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
