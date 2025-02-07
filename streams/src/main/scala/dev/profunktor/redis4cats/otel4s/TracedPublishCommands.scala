package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.all.*
import dev.profunktor.redis4cats.pubsub.PublishCommands
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

object TracedPublishCommands {

  /** Constructor for [[TracerProvider]]. */
  def apply[F[_]: Functor, S[_], K, V](
      cmd: PublishCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit tracerProvider: TracerProvider[F]): F[TracedPublishCommands[F, S, K, V]] = {
    tracerProvider
      .tracer("dev.profunktor.redis4cats.otel4s.TracedPublishCommands")
      .withVersion(buildinfo.BuildInfo.version)
      .get
      .map { implicit tracer =>
        fromTracer(cmd, config)
      }
  }

  /** Constructor for [[Tracer]]. */
  def fromTracer[F[_]: Tracer, S[_], K, V](
      cmd: PublishCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  ): TracedPublishCommands[F, S, K, V] = {
    new TracedPublishCommandsImpl(config, cmd)
  }
}

/** @note
  *   This trait currently does not export any additional operations, but exists to be consistent with
  *   [[TracedSubscribeCommands]].
  */
trait TracedPublishCommands[F[_], S[_], K, V] extends PublishCommands[F, S, K, V]

class TracedPublishCommandsImpl[F[_]: Tracer, S[_], K, V](
    config: TracedRedisConfig[F, K, V],
    val cmd: PublishCommands[F, S, K, V]
) extends WrappedPublishCommands[F, S, K, V]
    with TracedPublishCommands[F, S, K, V] {
  override def recordKey = config.recordKey
  override def recordValue = config.recordValue

  override def wrap[A](name: String, attributes: collection.immutable.Iterable[Attribute[?]])(fa: F[A]): F[A] =
    config.span(name, attributes)(fa)
}
