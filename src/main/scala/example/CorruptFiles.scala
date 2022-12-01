package example

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSession, functions => f}
import org.apache.spark.sql.expressions.Window

object CorruptFiles {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("corrupt-files")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = "Item MAP<String, MAP<String, String>>, _corrupt_record String"

    val exportedDf = spark.read
      .text("src/main/resources/exports")
      .withColumn("fileName", f.input_file_name())
      .withColumn("json", f.from_json(f.col("value"), StructType.fromDDL(schema)))
      .select("json.Item", "json._corrupt_record", "fileName")

    val itemDf = exportedDf.where(f.col("_corrupt_record").isNull)
    val corruptDf = exportedDf.where(f.col("_corrupt_record").isNotNull)

    val orderSpec = Window.orderBy("fileName", "isPrefix")
    val groupSpec = Window.partitionBy("group")

    val fixedDf = corruptDf
      .withColumn("isPrefix", f.col("_corrupt_record").startsWith("{"))
      .withColumn("rowNum", f.row_number().over(orderSpec))
      .withColumn("group", ((f.col("rowNum") - 1) / 2).cast("int"))
      .withColumn("list", f.collect_list("_corrupt_record").over(groupSpec))
      .withColumn("value", f.array_join(f.col("list"), ""))
      .where(f.col("rowNum") % 2 === 0)
      .withColumn("json", f.from_json(f.col("value"), StructType.fromDDL(schema)))
      .select("json.Item")

    val allItemDf = itemDf.select("Item").union(fixedDf.select("Item"))

    allItemDf.show(false)
    allItemDf.printSchema()

    spark.stop()
  }
}
