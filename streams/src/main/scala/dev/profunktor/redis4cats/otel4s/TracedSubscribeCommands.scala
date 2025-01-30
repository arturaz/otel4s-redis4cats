package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.effect.kernel.Resource
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
  def apply[F[_]: Functor, S[_[_], _], K, V](
      cmd: SubscribeCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit tracerProvider: TracerProvider[F], SFunctor: Functor[S[F, *]]): F[TracedSubscribeCommands[F, S, K, V]] = {
    tracerProvider
      .tracer("dev.profunktor.redis4cats.otel4s.TracedSubscribeCommands")
      .withVersion(buildinfo.BuildInfo.version)
      .get
      .map { implicit tracer =>
        fromTracer(cmd, config)
      }
  }

  /** Constructor for [[Tracer]]. */
  def fromTracer[F[_]: Tracer, S[_[_], _], K, V](
      cmd: SubscribeCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit SFunctor: Functor[S[F, *]]): TracedSubscribeCommands[F, S, K, V] = {
    new TracedSubscribeCommandsImplementation(config, cmd)
  }
}
trait TracedSubscribeCommands[F[_], S[_[_], _], K, V] extends SubscribeCommands[F, S, K, V] {

  /** As [[subscribe]] but returns a resource for each event that introduces a span when it is used. */
  def subscribeWithTracedEvents(channel: RedisChannel[K]): S[F, Resource[F, (SpanOps.Res[F], V)]]

  /** As [[psubscribe]] but returns a resource for each event that introduces a span when it is used. */
  def psubscribeWithTracedEvents(
      channel: RedisPattern[K]
  ): S[F, Resource[F, (SpanOps.Res[F], RedisPatternEvent[K, V])]]
}

class TracedSubscribeCommandsImplementation[F[_]: Tracer, S[_[_], _], K, V](
    config: TracedRedisConfig[F, K, V],
    cmd: SubscribeCommands[F, S, K, V]
)(implicit SFunctor: Functor[S[F, *]])
    extends TracedSubscribeCommands[F, S, K, V]
    with CoreHelpers[K, V] {
  import config.*
  private def Attributes = StreamsAttributes

  override def recordKey = config.recordKey
  override def recordValue = config.recordValue

  override def subscribe(channel: RedisChannel[K]): S[F, V] =
    cmd.subscribe(channel)

  override def subscribeWithTracedEvents(channel: RedisChannel[K]): S[F, Resource[F, (SpanOps.Res[F], V)]] =
    subscribe(channel).map { value =>
      spanBuilder(
        "event",
        keyAsAttribute(channel.underlying, Attributes.Channel).toList ::: valueAsAttribute(value).toList
      ).resource.map((_, value))
    }

  override def unsubscribe(channel: RedisChannel[K]): F[Unit] =
    span("unsubscribe", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(cmd.unsubscribe(channel))

  override def psubscribe(channel: RedisPattern[K]): S[F, RedisPatternEvent[K, V]] =
    cmd.psubscribe(channel)

  override def psubscribeWithTracedEvents(
      channel: RedisPattern[K]
  ): S[F, Resource[F, (SpanOps.Res[F], RedisPatternEvent[K, V])]] =
    psubscribe(channel).map { value =>
      val RedisPatternEvent(pattern, channel, data) = value

      spanBuilder(
        "event",
        keyAsAttribute(pattern, Attributes.Pattern).toList :::
          keyAsAttribute(channel, Attributes.Channel).toList :::
          valueAsAttribute(data, Attributes.Data).toList
      ).resource.map((_, value))
    }

  override def punsubscribe(channel: RedisPattern[K]): F[Unit] =
    span("punsubscribe", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(cmd.punsubscribe(channel))
}
