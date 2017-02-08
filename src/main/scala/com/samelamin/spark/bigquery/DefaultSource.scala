/*
 * Copyright (c) 2015 Samelamin, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.samelamin.spark.bigquery

import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.samelamin.spark.bigquery.converters.SchemaConverters
import com.samelamin.spark.bigquery.streaming.{BigQuerySink, BigQuerySource}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
/**
  * The default BigQuery source for Spark SQL.
  */
class DefaultSource
  extends StreamSinkProvider
    with StreamSourceProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String],
                          partitionColumns: Seq[String], outputMode: OutputMode): Sink = {

    val path = parameters.get("path").getOrElse("transaction_log")
    new BigQuerySink(sqlContext.sparkSession, path, parameters)

  }

  def getConvertedSchema(sqlContext: SQLContext,options: Map[String, String]): StructType = {
    val bigqueryClient = BigQueryClient.getInstance(sqlContext)
    val tableReference = BigQueryStrings.parseTableReference(options.get("tableReferenceSource").get)
    SchemaConverters.BQToSQLSchema(bigqueryClient.getTableSchema(tableReference))
  }

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            options: Map[String, String]): (String, StructType) = {
    val convertedSchema = getConvertedSchema(sqlContext,options)
    ("bigquery", schema.getOrElse(convertedSchema))
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    new BigQuerySource(sqlContext, schema, parameters)
  }
}
