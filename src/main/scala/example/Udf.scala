package example

import org.apache.spark.sql.{Column, functions => f}

import scala.reflect.runtime.universe.TypeTag

object Udf {
  def valueOf[T: TypeTag](col: String, path: String): Column = f
    .udf((input: String) => Helper.extract(input, path).asInstanceOf[T])
    .apply(f.col(col))
}
