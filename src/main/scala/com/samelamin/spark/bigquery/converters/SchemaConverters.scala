package com.samelamin.spark.bigquery.converters

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
import java.nio.ByteBuffer
import java.util.HashMap
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Builds BigQuery input JSON schema based on DataFrame.
  * Example schema can be found here: https://cloud.google.com/bigquery/docs/personsDataSchema.json
  */
object SchemaConverters {

  private def getMode(field: StructField) = {
    field.dataType match {
      case ArrayType(_, _) => "REPEATED"
      case _ => if (field.nullable) "NULLABLE" else "REQUIRED"
    }
  }
  private def getTypeName(dataType: DataType) = {
    dataType match {
      case ByteType | ShortType | IntegerType | LongType => "INTEGER"
      case StringType => "STRING"
      case FloatType | DoubleType => "FLOAT"
      case _: DecimalType => "FLOAT"
      case BinaryType => "BYTES"
      case BooleanType => "BOOLEAN"
      case TimestampType => "TIMESTAMP"
      case DateType => "DATE"
      case ArrayType(_, _) | MapType(_, _, _) | _: StructType => "RECORD"
      case _ => throw new RuntimeException (s"Couldn't match type $dataType")
    }
  }
  private def typeToJson(field: StructField, dataType: DataType): JValue = {
    dataType match {
      case structType: StructType =>
        ("type" -> getTypeName(dataType)) ~
          ("fields" -> structType.fields.map(fieldToJson(_)).toList)
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: ArrayType =>
            throw new IllegalArgumentException(s"Multidimensional arrays are not supported: ${field.name}")
          case other =>
            typeToJson(field, other)
        }
      case mapType: MapType =>
        throw new IllegalArgumentException(s"Unsupported type: ${dataType}")
      case other =>
        ("type" -> getTypeName(dataType))
    }
  }
  private def fieldToJson(field: StructField): JValue = {
    ("name" -> field.name) ~
      ("mode" -> getMode(field)) merge
      typeToJson(field, field.dataType)
  }
  def SqlToBQSchema(df: DataFrame): String = {
    pretty(render(JArray(df.schema.fields.map(fieldToJson(_)).toList)))
  }

  def getTypeName(dataType: String):DataType ={
    dataType match {
      case "INTEGER" => LongType
      case "FLOAT" => FloatType
      case "STRING" => StringType
      case "BYTES" => BinaryType
      case "BOOLEAN" => BooleanType
      case "TIMESTAMP" => TimestampType
    }

  }
  private def typeToStructField(field: TableFieldSchema, dataType:String): StructField = {
    dataType match {
      case "RECORD" => {
        val structFields = field.getFields().asScala.map(f=> typeToStructField(f,f.getType))
        val structType = StructType(structFields.toArray)
        if(field.getMode == "REPEATED") {
          new StructField(field.getName, ArrayType(structType))
        } else {
          new StructField(field.getName, structType)
        }
      }
      case other => {
        if(field.getMode == "REPEATED") {
          new StructField(field.getName, ArrayType(getTypeName(dataType)))
        } else {
          new StructField(field.getName, getTypeName(dataType))
        }
      }
    }
  }
  private def bqSchemaToStructType(tableSchema: TableSchema): StructType = {
    val bqSchemaFields = tableSchema.getFields().asScala.map(s=>typeToStructField(s,s.getType))
    val structType = StructType(bqSchemaFields.toArray)
    structType
  }
  def BQToSQLSchema(tableSchema: TableSchema): StructType = {
    bqSchemaToStructType(tableSchema)
  }

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * This function takes an avro schema and returns a sql schema.
    */
  def avroToSqlType(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case INT => SchemaType(IntegerType, nullable = false)
      case STRING => SchemaType(StringType, nullable = false)
      case BOOLEAN => SchemaType(BooleanType, nullable = false)
      case BYTES => SchemaType(BinaryType, nullable = false)
      case DOUBLE => SchemaType(DoubleType, nullable = false)
      case FLOAT => SchemaType(FloatType, nullable = false)
      case LONG => SchemaType(LongType, nullable = false)
      case FIXED => SchemaType(BinaryType, nullable = false)
      case ENUM => SchemaType(StringType, nullable = false)

      case RECORD =>
        val fields = avroSchema.getFields.map { f =>
          val schemaType = avroToSqlType(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }

        SchemaType(StructType(fields), nullable = false)

      case ARRAY =>
        val schemaType = avroToSqlType(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case MAP =>
        val schemaType = avroToSqlType(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case UNION =>
        if (avroSchema.getTypes.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            avroToSqlType(remainingUnionTypes.get(0)).copy(nullable = true)
          } else {
            avroToSqlType(Schema.createUnion(remainingUnionTypes)).copy(nullable = true)
          }
        } else avroSchema.getTypes.map(_.getType) match {
          case Seq(t1) =>
            avroToSqlType(avroSchema.getTypes.get(0))
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case other => throw new UnsupportedOperationException(
            s"This mix of union types is not supported (see README): $other")
        }

      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  /**
    * Returns a function that is used to convert avro types to their
    * corresponding sparkSQL representations.
    */
  def createConverterToSQL(schema: Schema): Any => Any = {
    schema.getType match {
      // Avro strings are in Utf8, so we have to call toString on them
      case STRING | ENUM => (item: Any) => if (item == null) null else item.toString
      case INT | BOOLEAN | DOUBLE | FLOAT | LONG => identity
      // Byte arrays are reused by avro, so we have to make a copy of them.
      case FIXED => (item: Any) => if (item == null) {
        null
      } else {
        item.asInstanceOf[Fixed].bytes().clone()
      }
      case BYTES => (item: Any) => if (item == null) {
        null
      } else {
        val bytes = item.asInstanceOf[ByteBuffer]
        val javaBytes = new Array[Byte](bytes.remaining)
        bytes.get(javaBytes)
        javaBytes
      }
      case RECORD =>
        val fieldConverters = schema.getFields.map(f => createConverterToSQL(f.schema))
        (item: Any) => if (item == null) {
          null
        } else {
          val record = item.asInstanceOf[GenericRecord]
          val converted = new Array[Any](fieldConverters.size)
          var idx = 0
          while (idx < fieldConverters.size) {
            converted(idx) = fieldConverters.apply(idx)(record.get(idx))
            idx += 1
          }
          Row.fromSeq(converted.toSeq)
        }
      case ARRAY =>
        val elementConverter = createConverterToSQL(schema.getElementType)
        (item: Any) => if (item == null) {
          null
        } else {
          item.asInstanceOf[GenericData.Array[Any]].map(elementConverter)
        }
      case MAP =>
        val valueConverter = createConverterToSQL(schema.getValueType)
        (item: Any) => if (item == null) {
          null
        } else {
          item.asInstanceOf[HashMap[Any, Any]].map(x => (x._1.toString, valueConverter(x._2))).toMap
        }
      case UNION =>
        if (schema.getTypes.exists(_.getType == NULL)) {
          val remainingUnionTypes = schema.getTypes.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            createConverterToSQL(remainingUnionTypes.get(0))
          } else {
            createConverterToSQL(Schema.createUnion(remainingUnionTypes))
          }
        } else schema.getTypes.map(_.getType) match {
          case Seq(t1) =>
            createConverterToSQL(schema.getTypes.get(0))
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
            (item: Any) => {
              item match {
                case l: Long => l
                case i: Int => i.toLong
                case null => null
              }
            }
          case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
            (item: Any) => {
              item match {
                case d: Double => d
                case f: Float => f.toDouble
                case null => null
              }
            }
          case other => throw new UnsupportedOperationException(
            s"This mix of union types is not supported (see README): $other")
        }
      case other => throw new UnsupportedOperationException(s"invalid avro type: $other")
    }
  }
}
