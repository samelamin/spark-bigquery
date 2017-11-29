package com.samelamin.spark.bigquery

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings
import com.samelamin.spark.bigquery.converters.SchemaConverters
import com.samelamin.spark.bigquery._

class BigQueryRelation(tableReferenceSource: String)(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  def schema: StructType = getConvertedSchema(sqlContext)

  def buildScan(): RDD[Row] = sqlContext.bigQueryTable(tableReferenceSource).rdd

  def getConvertedSchema(sqlContext: SQLContext): StructType = {
    val bigqueryClient = BigQueryClient.getInstance(sqlContext)
    val tableReference = BigQueryStrings.parseTableReference(tableReferenceSource)
    SchemaConverters.BQToSQLSchema(bigqueryClient.getTableSchema(tableReference))
  }

}
