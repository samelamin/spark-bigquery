spark-bigquery
===============

[![Build Status](https://travis-ci.org/samelamin/spark-bigquery.png)](https://travis-ci.org/samelamin/spark-bigquery)

This Spark module allows saving DataFrame as BigQuery table.

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
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.samelamin</groupId>
    <artifactId>spark-bigquery_${scala.binary.version}</artifactId>
    <version>0.1.0</version>
  </dependency>
</dependencies>
```

#### SBT

To use it in a local SBT console first add the package as a dependency then set up your project details
```sbt
resolvers += Opts.resolver.sonatypeSnapshots

libraryDependencies += "com.samelamin" %% "spark-bigquery" % "0.1.0"
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
      .option("tableSpec","my-project:my_dataset.my_table")
      .format("com.samelamin.spark.bigquery")
      .start()
```


### Saving DataFrame using BigQuery Hadoop writer API

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
 * Structured Streaming currently does not support schema updates

# License

Copyright 2016 samelamin.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
