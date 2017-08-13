package com.samelamin.spark

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by sam elamin on 28/01/2017.
  */
package object bigquery {

  object CreateDisposition extends Enumeration {
    val CREATE_IF_NEEDED, CREATE_NEVER = Value
  }

  object WriteDisposition extends Enumeration {
    val WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY = Value
  }

  implicit def makebigQueryContext(sql: SQLContext): BigQuerySQLContext =
       new BigQuerySQLContext(sql)

  implicit def makebigQueryDataFrame(self: DataFrame): BigQueryDataFrame =
    new BigQueryDataFrame(self)
}