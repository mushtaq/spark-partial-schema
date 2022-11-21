package example

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

import scala.annotation.tailrec

object Helper {
  private val mapper: ClassTagExtensions.Mixin = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)
    .build() :: ClassTagExtensions

  def extract[T](input: String, path: String): T = {
    input match {
      case null => null
      case _    => get(mapper.readValue(input), path.split("\\.").toList)
    }
  }.asInstanceOf[T]

  @tailrec
  private def get(input: Any, keys: List[String]): Any = (input, keys) match {
    case (x: Map[Any, Any], head :: tail) => get(x(head), tail)
    case _                                => input
  }
}
