package dev.profunktor.redis4cats.otel4s

import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.SpanBuilder
import org.typelevel.otel4s.trace.SpanOps
import org.typelevel.otel4s.trace.Tracer

import scala.collection.immutable

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
) { self =>

  /** Returns a span builder.
    *
    * @param name
    *   The name of the span.
    * @param attributes
    *   A collection of attributes to add to the span.
    */
  def spanBuilder(name: String, attributes: collection.immutable.Iterable[Attribute[?]] = Nil)(implicit
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
  def spanOps(name: String, attributes: collection.immutable.Iterable[Attribute[?]] = Nil)(implicit
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
  def span[A](name: String, attributes: collection.immutable.Iterable[Attribute[?]] = Nil)(fa: F[A])(implicit
      tracer: Tracer[F]
  ): F[A] =
    spanOps(name, attributes).surround(fa)

  def asCommandWrapper(implicit tracer: Tracer[F]): CommandWrapper[F] = new CommandWrapper[F] {
    override def wrap[A](name: String, attributes: immutable.Iterable[Attribute[?]])(fa: F[A]): F[A] =
      span(name, attributes)(fa)
  }

  def asWrappingHelpers: WrappingHelpers[K, V] = new WrappingHelpers[K, V] {
    override def recordKey = self.recordKey
    override def recordValue = self.recordValue
  }
}
