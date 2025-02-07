package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.functor.*
import dev.profunktor.redis4cats.RedisCommands
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

import scala.collection.immutable

object TracedRedisCommands {

  /** Constructor for [[TracerProvider]]. */
  def apply[F[_]: Functor, K, V](
      cmd: RedisCommands[F, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit tracerProvider: TracerProvider[F]): F[TracedRedisCommands[F, K, V]] = {
    tracerProvider
      .tracer("dev.profunktor.redis4cats.otel4s.TracedRedisCommands")
      .withVersion(buildinfo.BuildInfo.version)
      .get
      .map { implicit tracer =>
        fromTracer(cmd, config)
      }
  }

  /** Constructor for [[Tracer]]. */
  def fromTracer[F[_]: Tracer, K, V](
      cmd: RedisCommands[F, K, V],
      config: TracedRedisConfig[F, K, V]
  ): TracedRedisCommands[F, K, V] = {
    new TracedRedisCommandsImpl(config, cmd)
  }
}

/** This trait does not provide any extra operations but exists for consistency with the streaming API. */
trait TracedRedisCommands[F[_], K, V] extends RedisCommands[F, K, V]

class TracedRedisCommandsImpl[F[_]: Tracer, K, V](
    config: TracedRedisConfig[F, K, V],
    val cmd: RedisCommands[F, K, V]
) extends WrappedRedisCommands[F, K, V]
    with TracedRedisCommands[F, K, V] {
  override def recordKey = config.recordKey
  override def recordValue = config.recordValue

  override def wrap[A](name: String, attributes: immutable.Iterable[Attribute[?]])(fa: F[A]): F[A] =
    config.span(name, attributes)(fa)
}
