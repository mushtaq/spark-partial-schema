package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => f}

object Demo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .schema("Item MAP<String, MAP<String, String>>")
      .json("src/main/resources/inputList.json")

    df.show()

    import spark.implicits._

    val extractAppId = f.udf(Helper.extract[String](_, "app.M.id.S"))

    val df2 = df
      .where($"Item.partitionId.S" === "pid1")
      .withColumn("appid", extractAppId($"Item.custom_key1.M"))
      .where($"appid" === "app1")
      .drop("appid")

    df2.show()

    spark.stop()
  }

}
