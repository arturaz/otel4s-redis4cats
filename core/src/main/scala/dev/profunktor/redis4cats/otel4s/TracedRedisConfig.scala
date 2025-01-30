package dev.profunktor.redis4cats.otel4s

import org.typelevel.otel4s.trace.SpanBuilder
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.trace.SpanOps

/** @param configureSpanBuilder
  *   A function that configures a span builder.
  * @param recordKey
  *   A function that converts keys of a command to strings. If `None`, the keys will not be recorded.
  * @param recordValue
  *   A function that converts values of a command to strings. If `None`, the values will not be recorded.
  */
case class TracedRedisConfig[F[_], K, V](
    configureSpanBuilder: SpanBuilder[F] => SpanBuilder[F],
    recordKey: Option[K => String],
    recordValue: Option[V => String]
) {
  /** Returns a span builder.
    *
    * @param name
    *   The name of the span.
    * @param attributes
    *   A collection of attributes to add to the span.
    */
  def spanBuilder(name: String, attributes: collection.immutable.Iterable[Attribute[_]] = Nil)(implicit
      tracer: Tracer[F]
  ): SpanOps[F] =
    configureSpanBuilder(tracer.spanBuilder(name).addAttributes(attributes)).build

  /** Surrounds the `F[A]` with a span.
    *
    * @param name
    *   The name of the span.
    * @param attributes
    *   A collection of attributes to add to the span.
    */
  def span[A](name: String, attributes: collection.immutable.Iterable[Attribute[_]] = Nil)(fa: F[A])(implicit
      tracer: Tracer[F]
  ): F[A] =
    spanBuilder(name, attributes).surround(fa)
}
