package dev.profunktor.redis4cats.otel4s

import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.SpanBuilder
import org.typelevel.otel4s.trace.SpanOps
import org.typelevel.otel4s.trace.Tracer

/** @param spanName
  *   A function that converts a command name to a span name.
  * @param configureSpanBuilder
  *   A function that configures a span builder. The second argument is the command name (before passing it through
  *   `spanName`).
  * @param recordKey
  *   A function that converts keys of a command to strings. If `None`, the keys will not be recorded.
  * @param recordValue
  *   A function that converts values of a command to strings. If `None`, the values will not be recorded.
  */
case class TracedRedisConfig[F[_], K, V](
    spanName: String => String,
    configureSpanBuilder: (SpanBuilder[F], String) => SpanBuilder[F],
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
  ): SpanBuilder[F] =
    configureSpanBuilder(tracer.spanBuilder(spanName(name)).addAttributes(attributes), name)

  /** Returns a span builder.
    *
    * @param name
    *   The name of the span.
    * @param attributes
    *   A collection of attributes to add to the span.
    */
  def spanOps(name: String, attributes: collection.immutable.Iterable[Attribute[_]] = Nil)(implicit
      tracer: Tracer[F]
  ): SpanOps[F] =
    spanBuilder(name, attributes).build

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
    spanOps(name, attributes).surround(fa)
}
