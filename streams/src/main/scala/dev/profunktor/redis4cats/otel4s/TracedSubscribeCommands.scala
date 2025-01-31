package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.all.*
import dev.profunktor.redis4cats.data.RedisChannel
import dev.profunktor.redis4cats.data.RedisPattern
import dev.profunktor.redis4cats.data.RedisPatternEvent
import dev.profunktor.redis4cats.pubsub.SubscribeCommands
import org.typelevel.otel4s.trace.SpanOps
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.trace.TracerProvider

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
    new TracedSubscribeCommandsImplementation(config, cmd)
  }
}
trait TracedSubscribeCommands[F[_], S[_], K, V] extends SubscribeCommands[F, S, K, V] {

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

private class TracedSubscribeCommandsImplementation[F[_]: Tracer, S[_], K, V](
    config: TracedRedisConfig[F, K, V],
    cmd: SubscribeCommands[F, S, K, V]
)(implicit SFunctor: Functor[S[*]])
    extends TracedSubscribeCommands[F, S, K, V]
    with CoreHelpers[K, V] {
  import config.*
  private def Attributes = StreamsAttributes

  override def recordKey = config.recordKey
  override def recordValue = config.recordValue

  override def subscribe(channel: RedisChannel[K]): S[V] =
    cmd.subscribe(channel)

  override def subscribeWithTracedEvents(
      channel: RedisChannel[K],
      eventName: V => String
  ): S[(SpanOps[F], V)] =
    subscribe(channel).map { value =>
      val ops = spanOps(
        eventName(value),
        keyAsAttribute(channel.underlying, Attributes.Channel).toList ::: valueAsAttribute(value).toList
      )

      (ops, value)
    }

  override def unsubscribe(channel: RedisChannel[K]): F[Unit] =
    span("unsubscribe", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(cmd.unsubscribe(channel))

  override def psubscribe(channel: RedisPattern[K]): S[RedisPatternEvent[K, V]] =
    cmd.psubscribe(channel)

  override def psubscribeWithTracedEvents(
      channel: RedisPattern[K],
      eventName: RedisPatternEvent[K, V] => String
  ): S[(SpanOps[F], RedisPatternEvent[K, V])] =
    psubscribe(channel).map { value =>
      val RedisPatternEvent(pattern, channel, data) = value

      val ops = spanOps(
        eventName(value),
        keyAsAttribute(pattern, Attributes.Pattern).toList :::
          keyAsAttribute(channel, Attributes.Channel).toList :::
          valueAsAttribute(data, Attributes.Data).toList
      )

      (ops, value)
    }

  override def punsubscribe(channel: RedisPattern[K]): F[Unit] =
    span("punsubscribe", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(cmd.punsubscribe(channel))
}
