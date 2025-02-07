package dev.profunktor.redis4cats.otel4s

import org.typelevel.otel4s.Attribute

trait CommandWrapper[F[_]] {

  /** Wraps the command in a span with some attributes. */
  def wrap[A](name: String, attributes: collection.immutable.Iterable[Attribute[?]] = Nil)(fa: F[A]): F[A]
}
