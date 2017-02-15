Databricks Readme
==============

To be able to use this connector with Databricks you need to ensure the google credentials json file is saved locally

First download the jar from Maven Central, you can do this in [databricks](https://databricks.com/blog/2015/07/28/using-3rd-party-libraries-in-databricks-apache-spark-packages-and-maven-libraries.html)

You can add that via a init scipt, save it inside the /databricks/ folder,

For this example im saving the file to:

`/databricks/{google-json-credentials-file}.json`

Once you save the file locally you need to pass in the dependencies using
``` scala
    import com.samelamin.spark.bigquery._
    spark.sparkContext.hadoopConfiguration.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    val jsonFile = "/databricks/{google-json-credentials-file}.json"
    val BigQueryProjectId = "my-project"
    val GcsBucket = "my-bucket"
    sqlContext.setGcpJsonKeyFile(jsonFile)
    sqlContext.setGSProjectId(BQ_PROJECT_ID)
    sqlContext.setBigQueryProjectId(BQ_PROJECT_ID)
    sqlContext.setBigQueryGcsBucket(GcsBucket)
    sqlContext.setBigQueryDatasetLocation("EU")
```
