package example

import org.apache.spark.sql.{Column, SparkSession, functions => f}

import scala.reflect.runtime.universe.TypeTag

object Demo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .schema("Item MAP<String, MAP<String, String>>")
      .json("src/main/resources/inputList.json")

    df.show()

    val appId = extractColumn[String]("Item.custom_key1.M", "app.M.id.S")

    val df2 = df
      .where(f.col("Item.partitionId.S") === "pid1")
      .where(appId === "app1")

    df2.show()

    spark.stop()
  }

  def extractColumn[T: TypeTag](col: String, path: String): Column = f.udf(Helper.extract[T](_, path)).apply(f.col(col))
}
