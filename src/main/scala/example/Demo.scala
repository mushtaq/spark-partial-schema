package example

import org.apache.spark.sql.SparkSession

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

    df.printSchema()
    df.show()
    spark.stop()
  }

}
