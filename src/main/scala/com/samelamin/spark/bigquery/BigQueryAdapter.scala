/*
 * Copyright 2016 samelamin.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.samelamin.spark.bigquery

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
