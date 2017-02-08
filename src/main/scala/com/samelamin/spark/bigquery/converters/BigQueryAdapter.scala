package com.samelamin.spark.bigquery.converters

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  * Make DataFrame's schema valid for importing to BigQuery.
  * The rules are:
  *
  * 1) Valid characters are: letters, digits and underscores
  * 2) All letters must be lowercase
  */
object BigQueryAdapter {

  private def adaptName(name: String, siblings: Array[String]): String = {
    var newName = name.replaceAll("\\W", "_").toLowerCase
    if (!newName.equals(name)) {
      // Avoid duplicates:
      var counter = 1;
      while (!siblings.find(_.equals(newName)).isEmpty) {
        newName = newName + "_" + counter
        counter = counter + 1
      }
    }
    newName
  }

  private def adaptField(structField: StructField, parentType: StructType): StructField = {
    new StructField(adaptName(structField.name, parentType.fieldNames), adaptType(structField.dataType), structField.nullable)
  }

  private def adaptType(dataType: DataType): DataType = {
    dataType match {
      case structType: StructType =>
        new StructType(structType.fields.map(adaptField(_, structType)))
      case arrayType: ArrayType =>
        new ArrayType(adaptType(arrayType.elementType), arrayType.containsNull)
      case mapType: MapType =>
        new MapType(adaptType(mapType.keyType), adaptType(mapType.valueType), mapType.valueContainsNull)
      case other => other
    }
  }

  def apply(df: DataFrame): DataFrame = {
    df.sqlContext.createDataFrame(df.rdd, adaptType(df.schema).asInstanceOf[StructType])
  }
}
