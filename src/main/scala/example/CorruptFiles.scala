package example

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSession, functions => f}
import org.apache.spark.sql.expressions.{Window => W}

import scala.util.chaining.scalaUtilChainingOps

object CorruptFiles {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("corrupt-files")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = "Item MAP<String, MAP<String, String>>, _corrupt_record String"
    val json = f.from_json(f.col("value"), StructType.fromDDL(schema))

    val exportedDf = spark.read
      .text("src/main/resources/exports")
      .withColumn("json", json)
      .select("json.Item", "json._corrupt_record")

    val itemDf = exportedDf.where(f.col("_corrupt_record").isNull)
    val corruptDf = exportedDf.where(f.col("_corrupt_record").isNotNull)

    val fixedDf = corruptDf
      .withColumn("fileName", f.input_file_name())
      .withColumn("rowNum", f.row_number().over(W.orderBy("fileName")))
      .withColumn("group", ((f.col("rowNum") - 1) / 2).cast("int"))
      .withColumn("list", f.collect_list("_corrupt_record").over(W.partitionBy("group")))
      .withColumn("value", f.array_join(f.col("list"), ""))
      .withColumn("json", json)
      .where(f.col("rowNum") % 2 === 0)
      .select("json.Item")
      .tap(_.show(false))

    val allItemDf = itemDf.select("Item").union(fixedDf.select("Item"))

    allItemDf.show(false)
    allItemDf.printSchema()

    spark.stop()
  }
}
