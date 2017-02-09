package com.samelamin.spark.bigquery

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}

/**
  * Created by sam elamin on 29/01/2017.
  */
class DataFrameReaderFunctions(@transient val dfr: DataFrameReader) extends Serializable {
  private val source = "com.samelamin.spark.bigquery.DefaultSource"
  def bigquery(options: Map[String, String]): DataFrame =
    buildFrame(options, null)

  /**
    * Helper method to create the DataFrame.
    *
    * @param schema the manual schema defined.
    */
  private def buildFrame(options: Map[String, String] = null, schema: StructType = null): DataFrame = {
    val builder = dfr
      .format(source)
      .schema(schema)

    if (options != null) {
      builder.options(options)
    }

    builder.load()
  }
}
