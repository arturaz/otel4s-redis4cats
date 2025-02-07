package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.all.*
import dev.profunktor.redis4cats.pubsub.PublishCommands
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
    new TracedPublishCommandsImpl(cmd, config.asWrappingHelpers, config.asCommandWrapper)
  }
}

/** @note
  *   This trait currently does not export any additional operations, but exists to be consistent with
  *   [[TracedSubscribeCommands]].
  */
trait TracedPublishCommands[F[_], S[_], K, V] extends PublishCommands[F, S, K, V] with TracedModifiers[F, K, V] {

  override type Self <: TracedPublishCommands[F, S, K, V]
}

// No stable ABI guaranteed
private class TracedPublishCommandsImpl[F[_]: Tracer, S[_], K, V](
    val cmd: PublishCommands[F, S, K, V],
    val helpers: WrappingHelpers[K, V],
    val wrapper: CommandWrapper[F]
) extends WrappedPublishCommands[F, S, K, V]
    with TracedPublishCommands[F, S, K, V] {
  override type Self = TracedPublishCommands[F, S, K, V]

  /** Modifies the current [[WrappingHelpers]]. */
  override def withHelpers(f: WrappingHelpers[K, V] => WrappingHelpers[K, V]): TracedPublishCommands[F, S, K, V] =
    new TracedPublishCommandsImpl(cmd, f(helpers), wrapper)

  /** Modifies the current [[CommandWrapper]]. */
  override def withWrapper(f: CommandWrapper[F] => CommandWrapper[F]): TracedPublishCommands[F, S, K, V] =
    new TracedPublishCommandsImpl(cmd, helpers, f(wrapper))
}
