package example

import org.apache.spark.sql.{SparkSession, functions => f}

object Approach1Udf {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .schema("Item MAP<String, MAP<String, String>>")
      .json("src/main/resources/input.json.gz")

    df.show()

    val df2 = df
      .where(f.col("Item.partitionId.S") === "pid1")
      .where(Udf.valueOf[String]("Item.custom_key1.M", "app.M.id.S") === "app1")

    df2.show()

    spark.stop()
  }

}
