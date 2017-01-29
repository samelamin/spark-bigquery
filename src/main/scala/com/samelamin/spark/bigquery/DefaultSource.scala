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

import com.samelamin.spark.bigquery.streaming.{BigQuerySink, BigQuerySource}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
/**
  * The default source for Spark SQL.
  */
class DefaultSource
  extends StreamSinkProvider
    with StreamSourceProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String],
                          partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    new BigQuerySink(parameters)

  }

  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             options: Map[String, String]): (String, StructType) = {
    ("bigquery", schema.getOrElse(BigQuerySource.DEFAULT_SCHEMA))
  }


  override def createSource(sqlContext: SQLContext, metadataPath: String,
                            schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    new BigQuerySource(sqlContext, schema, parameters)
  }
}
object DefaultSource {
  val DEFAULT_STREAM_FROM: String = "BEGINNING"
  val DEFAULT_STREAM_TO: String = "INFINITY"
  val DEFAULT_DOCUMENT_ID_FIELD: String = "META_ID"
}
