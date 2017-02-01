package com.samelamin.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.samelamin.spark.bigquery.BigQuerySchema
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class BigQuerySchemaSpecs extends FeatureSpec with GivenWhenThen with DataFrameSuiteBase {
  feature("Schema Converters. Dataframe To BQ Schema") {
    scenario("When converting a simple dataframe") {
      Given("A dataframe")
      val sampleJson = """{
                         |	"id": 1,
                         |	"error": null
                         |}""".stripMargin
      val df = sqlContext.read.json(sc.parallelize(List(sampleJson)))

      When("Passing the schema to the converter")
      val tableSchema = BigQuerySchema(df)

      Then("We should receive a BQ Table Schema")
      val expectedSchema = """[ {
                             |  "name" : "error",
                             |  "mode" : "NULLABLE",
                             |  "type" : "STRING"
                             |}, {
                             |  "name" : "id",
                             |  "mode" : "NULLABLE",
                             |  "type" : "INTEGER"
                             |} ]
                             |""".stripMargin.trim

      tableSchema should not be null
      tableSchema should be (expectedSchema)

    }

    scenario("When converting a complex dataframe with nested data") {
      Given("A dataframe")
      val sqlCtx = sqlContext
      val sampleNestedJson = """{
                               |	"id": 1,
                               |	"error": null,
                               |	"result": {
                               |		"nPeople": 2,
                               |		"people": [{
                               |			"namePeople": "Inca",
                               |			"power": "1235",
                               |			"location": "asdfghjja",
                               |			"idPeople": 189,
                               |			"mainItems": "brownGem",
                               |			"verified": false,
                               |			"description": "Lorem impsum bla bla",
                               |			"linkAvatar": "avatar_12.jpg",
                               |			"longitude": 16.2434263,
                               |			"latitude": 89.355118
                               |		}, {
                               |			"namePeople": "Maya",
                               |			"power": "1235",
                               |			"location": "hcjkjhljhl",
                               |			"idPeople": 119,
                               |			"mainItems": "greenstone",
                               |			"verified": false,
                               |			"description": "Lorem impsum bla bla",
                               |			"linkAvatar": "avatar_6.jpg",
                               |			"longitude": 16.2434263,
                               |			"latitude": 89.3551185
                               |		}]
                               |	}
                               |}""".stripMargin
      val df = sqlContext.read.json(sc.parallelize(List(sampleNestedJson)))

      When("Passing the schema to the converter")
      val tableSchema = BigQuerySchema(df)

      Then("We should receive a BQ Table Schema")
      tableSchema should not be null

      val expectedSchema = """[ {
                             |  "name" : "error",
                             |  "mode" : "NULLABLE",
                             |  "type" : "STRING"
                             |}, {
                             |  "name" : "id",
                             |  "mode" : "NULLABLE",
                             |  "type" : "INTEGER"
                             |}, {
                             |  "name" : "result",
                             |  "mode" : "NULLABLE",
                             |  "type" : "RECORD",
                             |  "fields" : [ {
                             |    "name" : "nPeople",
                             |    "mode" : "NULLABLE",
                             |    "type" : "INTEGER"
                             |  }, {
                             |    "name" : "people",
                             |    "mode" : "REPEATED",
                             |    "type" : "RECORD",
                             |    "fields" : [ {
                             |      "name" : "description",
                             |      "mode" : "NULLABLE",
                             |      "type" : "STRING"
                             |    }, {
                             |      "name" : "idPeople",
                             |      "mode" : "NULLABLE",
                             |      "type" : "INTEGER"
                             |    }, {
                             |      "name" : "latitude",
                             |      "mode" : "NULLABLE",
                             |      "type" : "FLOAT"
                             |    }, {
                             |      "name" : "linkAvatar",
                             |      "mode" : "NULLABLE",
                             |      "type" : "STRING"
                             |    }, {
                             |      "name" : "location",
                             |      "mode" : "NULLABLE",
                             |      "type" : "STRING"
                             |    }, {
                             |      "name" : "longitude",
                             |      "mode" : "NULLABLE",
                             |      "type" : "FLOAT"
                             |    }, {
                             |      "name" : "mainItems",
                             |      "mode" : "NULLABLE",
                             |      "type" : "STRING"
                             |    }, {
                             |      "name" : "namePeople",
                             |      "mode" : "NULLABLE",
                             |      "type" : "STRING"
                             |    }, {
                             |      "name" : "power",
                             |      "mode" : "NULLABLE",
                             |      "type" : "STRING"
                             |    }, {
                             |      "name" : "verified",
                             |      "mode" : "NULLABLE",
                             |      "type" : "BOOLEAN"
                             |    } ]
                             |  } ]
                             |} ]
                             |""".stripMargin.trim
      tableSchema should be (expectedSchema)
    }
  }

  feature("Schema Converters. BQ Schema to Dataframe") {
    scenario("When converting a complete nested BQ Schema") {
      val bqSchema = """{"fields":[{"mode":"NULLABLE","name":"customerid","type":"INTEGER"},{"mode":"NULLABLE","name":"id","type":"STRING"},{"mode":"NULLABLE","name":"legacyorderid","type":"INTEGER"},{"mode":"NULLABLE","name":"notifiycustomer","type":"BOOLEAN"},{"fields":[{"fields":[{"mode":"NULLABLE","name":"applicationname","type":"STRING"},{"mode":"NULLABLE","name":"applicationversion","type":"STRING"},{"mode":"NULLABLE","name":"clientip","type":"STRING"},{"mode":"NULLABLE","name":"jefeature","type":"STRING"},{"mode":"NULLABLE","name":"useragent","type":"STRING"}],"mode":"NULLABLE","name":"applicationinfo","type":"RECORD"},{"fields":[{"mode":"NULLABLE","name":"basketid","type":"STRING"},{"mode":"NULLABLE","name":"deliverycharge","type":"FLOAT"},{"mode":"NULLABLE","name":"discount","type":"FLOAT"},{"mode":"REPEATED","name":"discounts","type":"STRING"},{"fields":[{"mode":"NULLABLE","name":"combinedprice","type":"FLOAT"},{"mode":"NULLABLE","name":"description","type":"STRING"},{"mode":"REPEATED","name":"discounts","type":"STRING"},{"mode":"REPEATED","name":"mealparts","type":"STRING"},{"mode":"NULLABLE","name":"menucardnumber","type":"STRING"},{"mode":"REPEATED","name":"multibuydiscounts","type":"STRING"},{"mode":"NULLABLE","name":"name","type":"STRING"},{"mode":"REPEATED","name":"optionalaccessories","type":"STRING"},{"mode":"NULLABLE","name":"productid","type":"INTEGER"},{"mode":"NULLABLE","name":"producttypeid","type":"INTEGER"},{"fields":[{"mode":"NULLABLE","name":"groupid","type":"INTEGER"},{"mode":"NULLABLE","name":"name","type":"STRING"},{"mode":"NULLABLE","name":"requiredaccessoryid","type":"INTEGER"},{"mode":"NULLABLE","name":"unitprice","type":"FLOAT"}],"mode":"REPEATED","name":"requiredaccessories","type":"RECORD"},{"mode":"NULLABLE","name":"synonym","type":"STRING"},{"mode":"NULLABLE","name":"unitprice","type":"FLOAT"}],"mode":"REPEATED","name":"items","type":"RECORD"},{"mode":"NULLABLE","name":"menuid","type":"INTEGER"},{"mode":"NULLABLE","name":"multibuydiscount","type":"FLOAT"},{"mode":"NULLABLE","name":"subtotal","type":"FLOAT"},{"mode":"NULLABLE","name":"tospend","type":"FLOAT"},{"mode":"NULLABLE","name":"total","type":"FLOAT"}],"mode":"NULLABLE","name":"basketinfo","type":"RECORD"},{"fields":[{"mode":"NULLABLE","name":"address","type":"STRING"},{"mode":"NULLABLE","name":"city","type":"STRING"},{"mode":"NULLABLE","name":"email","type":"STRING"},{"mode":"NULLABLE","name":"id","type":"STRING"},{"mode":"NULLABLE","name":"name","type":"STRING"},{"mode":"NULLABLE","name":"phonenumber","type":"STRING"},{"mode":"NULLABLE","name":"postcode","type":"STRING"},{"mode":"NULLABLE","name":"previousjeordercount","type":"INTEGER"},{"mode":"NULLABLE","name":"previousrestuarantordercount","type":"INTEGER"},{"mode":"NULLABLE","name":"timezone","type":"STRING"}],"mode":"NULLABLE","name":"customerinfo","type":"RECORD"},{"mode":"NULLABLE","name":"id","type":"STRING"},{"mode":"NULLABLE","name":"islocked","type":"BOOLEAN"},{"mode":"NULLABLE","name":"legacyid","type":"INTEGER"},{"fields":[{"mode":"NULLABLE","name":"duedate","type":"STRING"},{"mode":"NULLABLE","name":"duedatewithutcoffset","type":"STRING"},{"mode":"NULLABLE","name":"initialduedate","type":"STRING"},{"mode":"NULLABLE","name":"initialduedatewithutcoffset","type":"STRING"},{"mode":"NULLABLE","name":"notetorestaurant","type":"STRING"},{"mode":"NULLABLE","name":"orderable","type":"BOOLEAN"},{"mode":"NULLABLE","name":"placeddate","type":"STRING"},{"mode":"NULLABLE","name":"promptasap","type":"BOOLEAN"},{"mode":"NULLABLE","name":"servicetype","type":"STRING"}],"mode":"NULLABLE","name":"order","type":"RECORD"},{"fields":[{"mode":"NULLABLE","name":"drivertipvalue","type":"FLOAT"},{"mode":"NULLABLE","name":"orderid","type":"STRING"},{"mode":"NULLABLE","name":"paiddate","type":"STRING"},{"fields":[{"mode":"NULLABLE","name":"cardfee","type":"FLOAT"},{"mode":"NULLABLE","name":"cardtype","type":"STRING"},{"mode":"NULLABLE","name":"paymenttransactionref","type":"STRING"},{"mode":"NULLABLE","name":"pspname","type":"STRING"},{"mode":"NULLABLE","name":"type","type":"STRING"},{"mode":"NULLABLE","name":"value","type":"FLOAT"}],"mode":"REPEATED","name":"paymentlines","type":"RECORD"},{"mode":"NULLABLE","name":"total","type":"FLOAT"},{"mode":"NULLABLE","name":"totalcomplementary","type":"FLOAT"}],"mode":"NULLABLE","name":"paymentinfo","type":"RECORD"},{"fields":[{"mode":"REPEATED","name":"addresslines","type":"STRING"},{"mode":"NULLABLE","name":"city","type":"STRING"},{"mode":"NULLABLE","name":"dispatchmethod","type":"STRING"},{"mode":"NULLABLE","name":"id","type":"STRING"},{"mode":"NULLABLE","name":"latitude","type":"FLOAT"},{"mode":"NULLABLE","name":"longitude","type":"FLOAT"},{"mode":"NULLABLE","name":"name","type":"STRING"},{"mode":"NULLABLE","name":"offline","type":"BOOLEAN"},{"mode":"NULLABLE","name":"phonenumber","type":"STRING"},{"mode":"NULLABLE","name":"postcode","type":"STRING"},{"mode":"NULLABLE","name":"seoname","type":"STRING"},{"mode":"NULLABLE","name":"tempoffline","type":"BOOLEAN"}],"mode":"NULLABLE","name":"restaurantinfo","type":"RECORD"}],"mode":"NULLABLE","name":"ordercontainer","type":"RECORD"},{"mode":"NULLABLE","name":"orderid","type":"STRING"},{"mode":"NULLABLE","name":"orderresolutionstatus","type":"STRING"},{"mode":"NULLABLE","name":"raisingcomponent","type":"STRING"},{"mode":"NULLABLE","name":"restaurantid","type":"INTEGER"},{"mode":"NULLABLE","name":"tenant","type":"STRING"},{"mode":"NULLABLE","name":"timestamp","type":"STRING"}]}
                       |""".stripMargin





    }
  }
}