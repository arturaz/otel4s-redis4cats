package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.all.*
import dev.profunktor.redis4cats.data.RedisChannel
import dev.profunktor.redis4cats.pubsub.PublishCommands
import dev.profunktor.redis4cats.pubsub.data.Subscription
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.trace.TracerProvider

object TracedPublishCommands {

  /** Constructor for [[TracerProvider]]. */
  def apply[F[_]: Functor, S[_[_], _], K, V](
      cmd: PublishCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit tracerProvider: TracerProvider[F]): F[PublishCommands[F, S, K, V]] = {
    tracerProvider
      .tracer("dev.profunktor.redis4cats.otel4s.TracedPublishCommands")
      .withVersion(buildinfo.BuildInfo.version)
      .get
      .map { implicit tracer =>
        fromTracer(cmd, config)
      }
  }

  /** Constructor for [[Tracer]]. */
  def fromTracer[F[_]: Tracer, S[_[_], _], K, V](
      cmd: PublishCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  ): PublishCommands[F, S, K, V] = {
    new TracedPublishCommandsImplementation(config, cmd)
  }
}
trait TracedPublishCommands[F[_], S[_[_], _], K, V] extends PublishCommands[F, S, K, V]
class TracedPublishCommandsImplementation[F[_]: Tracer, S[_[_], _], K, V](
    config: TracedRedisConfig[F, K, V],
    cmd: PublishCommands[F, S, K, V]
) extends TracedPublishCommands[F, S, K, V]
    with CoreHelpers[K, V] {
  import config.*
  private def Attributes = StreamsAttributes

  override def recordKey = config.recordKey
  override def recordValue = config.recordValue

  override def numPat: F[Long] = span("numPat")(cmd.numPat)

  override def numSub: F[List[Subscription[K]]] = span("numSub")(cmd.numSub)

  override def pubSubChannels: F[List[RedisChannel[K]]] = span("pubSubChannels")(cmd.pubSubChannels)

  override def pubSubShardChannels: F[List[RedisChannel[K]]] = span("pubSubShardChannels")(cmd.pubSubShardChannels)

  override def pubSubSubscriptions(channel: RedisChannel[K]): F[Option[Subscription[K]]] =
    span("pubSubSubscriptions", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(
      cmd.pubSubSubscriptions(channel)
    )

  override def pubSubSubscriptions(channels: List[RedisChannel[K]]): F[List[Subscription[K]]] =
    span("pubSubSubscriptions", keys2AsAttribute(channels.iterator.map(_.underlying), Attributes.Channels).toList)(
      cmd.pubSubSubscriptions(channels)
    )

  override def shardNumSub(channels: List[RedisChannel[K]]): F[List[Subscription[K]]] =
    span("shardNumSub", keys2AsAttribute(channels.iterator.map(_.underlying), Attributes.Channels).toList)(
      cmd.shardNumSub(channels)
    )

  override def publish(channel: RedisChannel[K]): S[F, V] => S[F, Unit] = cmd.publish(channel)

  override def publish(channel: RedisChannel[K], value: V): F[Unit] =
    span(
      "publish",
      keyAsAttribute(channel.underlying, Attributes.Channel).toList :::
        valueAsAttribute(value, Attributes.Value).toList
    )(cmd.publish(channel, value))
}
