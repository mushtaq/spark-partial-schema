package example

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

import scala.annotation.tailrec

object Helper {
  val mapper: ClassTagExtensions.Mixin = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)
    .build() :: ClassTagExtensions

  @tailrec
  def get[T](input: Any, keys: List[String]): T = input match {
    case null => null.asInstanceOf[T]
    case _ =>
      keys match {
        case ::(head, next) => get(input.asInstanceOf[Map[String, Object]](head), next)
        case Nil            => input.asInstanceOf[T]
      }
  }

  def extract[T](input: String, path: String): T = input match {
    case null => null.asInstanceOf[T]
    case _    => get[T](mapper.readValue(input), path.split("\\.").toList)
  }
}
