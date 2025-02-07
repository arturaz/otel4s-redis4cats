package dev.profunktor.redis4cats.otel4s

import cats.data.Writer
import munit.CatsEffectSuite
import org.typelevel.otel4s.Attribute

import scala.collection.immutable

class CommandWrapperTest extends CatsEffectSuite {
  test("combine: proper order") {
    val wrapper1 = new CommandWrapper[Writer[Vector[String], *]] {
      override def wrap[A](name: String, attributes: immutable.Iterable[Attribute[?]])(fa: Writer[Vector[String], A]) =
        fa.tell(Vector(s"wrapper1: $name"))
    }

    val wrapper2 = new CommandWrapper[Writer[Vector[String], *]] {
      override def wrap[A](name: String, attributes: immutable.Iterable[Attribute[?]])(fa: Writer[Vector[String], A]) =
        fa.tell(Vector(s"wrapper2: $name"))
    }

    val combined = wrapper1.combine(wrapper2)

    val result = combined.wrap("name")(Writer.value("result"))

    assertEquals(result, Writer(Vector("wrapper1: name", "wrapper2: name"), "result"))
  }
}
