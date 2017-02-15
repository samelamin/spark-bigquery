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

name := "spark-bigquery"
organization := "com.github.samelamin"
scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")

spName := "samelamin/spark-bigquery"
sparkVersion := "2.1.0"
sparkComponents := Seq("core", "sql","streaming")
spAppendScalaVersion := false
spIncludeMaven := true
spIgnoreProvided := true
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
parallelExecution in Test := false
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-hive" % "2.1.0" % "test",
  "com.databricks" %% "spark-avro" % "3.1.0",
  "com.holdenkarau" %% "spark-testing-base" % "2.0.0_0.4.7" % "test",
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.8.0-hadoop2"
    exclude ("com.google.guava", "guava-jdk5"),
  "joda-time" % "joda-time" % "2.9.3",
  "org.mockito" % "mockito-core" % "1.8.5" % "test",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test"
)

// Release settings
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
releaseCrossBuild             := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
pomExtra                      := {
  <url>https://github.com/samelamin/spark-bigquery</url>
  <scm>
    <url>git@github.com/samelamin/spark-bigquery.git</url>
    <connection>scm:git:git@github.com:samelamin/spark-bigquery.git</connection>
  </scm>
  <developers>
    <developer>
      <id>samelamin</id>
      <name>Sam Elamin</name>
      <email>hussam.elamin@gmail.com</email>
      <url>https://github.com/samelamin</url>
    </developer>
  </developers>
}
