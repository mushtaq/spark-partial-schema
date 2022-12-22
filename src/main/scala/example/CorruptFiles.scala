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
      .select("json.Item", "json._corrupt_record", "value")

    val isItem = f.col("Item").isNotNull
    val isCorruptRecord = f.col("_corrupt_record").isNotNull
    val hasWrongSchema = f.not(isItem || isCorruptRecord)

    val itemDf = exportedDf
      .where(isItem)
      .select("Item")

    val corruptDf =
      exportedDf
        .where(hasWrongSchema)
        .select(f.col("value").as("_corrupt_record"))
        .union(
          exportedDf
            .where(isCorruptRecord)
            .select("_corrupt_record")
        )

    val fixedDf = corruptDf
      .withColumn("fileName", f.input_file_name())
      .withColumn("rowNum", f.row_number().over(W.orderBy("fileName")))
      .drop("fileName")
      .tap(_.show(false))
      .withColumn("list", f.collect_list("_corrupt_record").over(W.rowsBetween(0, 1)))
      .where(f.col("rowNum") % 2 =!= 0)
      .withColumn("value", f.array_join(f.col("list"), ""))
      .tap(_.show(false))
      .withColumn("json", json)
      .select("json.Item")
      .tap(_.show(false))

    val allItemDf = itemDf.select("Item").union(fixedDf.select("Item"))

    allItemDf.show(false)
    allItemDf.printSchema()

    spark.stop()
  }
}
