package example

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => f}

object Approach2Batching {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("batching-approach")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val format = "parquet" // change this to delta to enable mergeSchema on write as commented below
    val outputDataPath = "/tmp/data"
    // batches = total data / workload that does not OOM
    // batches = 1000 MB / 10 MB = 100
    val batches = 3

    // read input json as plain text so that there is no schema inference
    val inputDf: Dataset[String] = spark.read
      .text("src/main/resources/inputList.json")
      .map(_.getString(0))

    // split the large dataframe into smaller ones, choosing appropriate number of batches
    // then read each smaller dataframe as json with schema inference
    val jsonDfs: Array[DataFrame] = inputDf
      .randomSplit(Array.fill(batches)(6)) // for large data instead of weight of 6 use 1
      .map(spark.read.json)

    // write each json dataframe into the same location in append only mode and keep merging schemas
    jsonDfs.foreach { jsonDf =>
      jsonDf.write
        .format(format)
        .option("mergeSchema", "true") // this is used only when format is delta
        .mode("append")
        .save(outputDataPath)
    }

    // read the complete dataframe from the saved location to confirm that schema merging is complete
    val readData = spark.read
      .option("mergeSchema", "true") // this is not required when format is delta
      .format(format)
      .load(outputDataPath)

    readData.printSchema()
    readData.show()

    spark.stop()
  }
}
