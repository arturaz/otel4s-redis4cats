package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.all.*
import dev.profunktor.redis4cats.data.{RedisChannel, RedisPattern, RedisPatternEvent}
import dev.profunktor.redis4cats.pubsub.SubscribeCommands
import org.typelevel.otel4s.trace.{SpanOps, Tracer, TracerProvider}

object TracedSubscribeCommands {

  /** Constructor for [[TracerProvider]]. */
  def apply[F[_]: Functor, S[_], K, V](
      cmd: SubscribeCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit tracerProvider: TracerProvider[F], SFunctor: Functor[S[*]]): F[TracedSubscribeCommands[F, S, K, V]] = {
    tracerProvider
      .tracer("dev.profunktor.redis4cats.otel4s.TracedSubscribeCommands")
      .withVersion(buildinfo.BuildInfo.version)
      .get
      .map { implicit tracer =>
        fromTracer(cmd, config)
      }
  }

  /** Constructor for [[Tracer]]. */
  def fromTracer[F[_]: Tracer, S[_], K, V](
      cmd: SubscribeCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit SFunctor: Functor[S[*]]): TracedSubscribeCommands[F, S, K, V] = {
    new TracedSubscribeCommandsImpl(config, cmd, config.asWrappingHelpers, config.asCommandWrapper)
  }
}

/** Provides extra operations regarding tracing. */
trait TracedSubscribeCommands[F[_], S[_], K, V] extends SubscribeCommands[F, S, K, V] with TracedModifiers[F, K, V] {

  override type Self <: TracedSubscribeCommands[F, S, K, V]

  /** As [[subscribe]] but returns `SpanOps` for each event that introduces a span when it is used.
    *
    * You must use the `SpanOps` yourself to record the span.
    */
  def subscribeWithTracedEvents(
      channel: RedisChannel[K],
      eventName: V => String = _ => "event"
  ): S[(SpanOps[F], V)]

  /** As [[psubscribe]] but returns `SpanOps` for each event that introduces a span when it is used.
    *
    * You must use the `SpanOps` yourself to record the span.
    */
  def psubscribeWithTracedEvents(
      channel: RedisPattern[K],
      eventName: RedisPatternEvent[K, V] => String = _ => "event"
  ): S[(SpanOps[F], RedisPatternEvent[K, V])]
}

// No stable ABI guaranteed
private class TracedSubscribeCommandsImpl[F[_]: Tracer, S[_], K, V](
    config: TracedRedisConfig[F, K, V],
    val cmd: SubscribeCommands[F, S, K, V],
    val helpers: WrappingHelpers[K, V],
    val wrapper: CommandWrapper[F]
)(implicit SFunctor: Functor[S[*]])
    extends WrappedSubscribeCommands[F, S, K, V]
    with TracedSubscribeCommands[F, S, K, V] {
  override type Self = TracedSubscribeCommands[F, S, K, V]

  private def Attributes = StreamsAttributes
  import helpers.*

  /** Modifies the current [[WrappingHelpers]]. */
  override def withHelpers(f: WrappingHelpers[K, V] => WrappingHelpers[K, V]): TracedSubscribeCommands[F, S, K, V] =
    new TracedSubscribeCommandsImpl(config, cmd, f(helpers), wrapper)

  /** Modifies the current [[CommandWrapper]]. */
  override def withWrapper(f: CommandWrapper[F] => CommandWrapper[F]): TracedSubscribeCommands[F, S, K, V] =
    new TracedSubscribeCommandsImpl(config, cmd, helpers, f(wrapper))

  override def subscribeWithTracedEvents(
      channel: RedisChannel[K],
      eventName: V => String
  ): S[(SpanOps[F], V)] =
    subscribe(channel).map { value =>
      val ops = config.spanOps(
        eventName(value),
        keyAsAttribute(channel.underlying, Attributes.Channel).toList ::: valueAsAttribute(value).toList
      )

      (ops, value)
    }

  override def psubscribeWithTracedEvents(
      channel: RedisPattern[K],
      eventName: RedisPatternEvent[K, V] => String
  ): S[(SpanOps[F], RedisPatternEvent[K, V])] =
    psubscribe(channel).map { value =>
      val RedisPatternEvent(pattern, channel, data) = value

      val ops = config.spanOps(
        eventName(value),
        keyAsAttribute(pattern, Attributes.Pattern).toList :::
          keyAsAttribute(channel, Attributes.Channel).toList :::
          valueAsAttribute(data, Attributes.Data).toList
      )

      (ops, value)
    }
}
