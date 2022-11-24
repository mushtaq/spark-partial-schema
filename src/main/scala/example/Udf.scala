package example

import org.apache.spark.sql.{Column, functions => f}

import scala.reflect.runtime.universe.TypeTag

object Udf {
  def valueOf[T: TypeTag](col: String, path: String): Column = f.udf(Helper.extract[T](_, path)).apply(f.col(col))
}
