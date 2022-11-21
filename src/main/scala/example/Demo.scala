package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => f}

import java.nio.file.{Files, Paths}

object Demo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = Files.readString(Paths.get("src/main/resources/schemaLikePython.ddl"))

    val df = spark.read
      .schema(schema)
      .json("src/main/resources/inputList.json")

    df.show()

    import Helper._
    import spark.implicits._

    val extractUDF = f.udf(extract[String] _)

    val df2 = df
      .where($"Item.partitionId.S" === "pid1")
      .withColumn("appid", extractUDF($"Item.custom_key1.M", f.lit("app.M.id.S")))
      .where($"appid" === "app1")
      .drop("appid")

    df2.show()

    spark.stop()
  }

}
