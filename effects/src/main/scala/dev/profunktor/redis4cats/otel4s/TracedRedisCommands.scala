package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.functor.*
import dev.profunktor.redis4cats.RedisCommands
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

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
    new TracedRedisCommandsImpl(cmd, config.asWrappingHelpers, config.asCommandWrapper)
  }
}

trait TracedRedisCommands[F[_], K, V] extends RedisCommands[F, K, V] with TracedModifiers[F, K, V] {
  override type Self = TracedRedisCommands[F, K, V]
}

// No ABI stability guaranteed
private class TracedRedisCommandsImpl[F[_]: Tracer, K, V](
    val cmd: RedisCommands[F, K, V],
    val helpers: WrappingHelpers[K, V],
    val wrapper: CommandWrapper[F]
) extends WrappedRedisCommands[F, K, V]
    with TracedRedisCommands[F, K, V] {
  override def withHelpers(f: WrappingHelpers[K, V] => WrappingHelpers[K, V]) =
    new TracedRedisCommandsImpl(cmd, f(helpers), wrapper)

  override def withWrapper(f: CommandWrapper[F] => CommandWrapper[F]) =
    new TracedRedisCommandsImpl(cmd, helpers, f(wrapper))
}
