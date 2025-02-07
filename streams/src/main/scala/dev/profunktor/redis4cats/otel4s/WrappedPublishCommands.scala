package dev.profunktor.redis4cats.otel4s

import dev.profunktor.redis4cats.data.RedisChannel
import dev.profunktor.redis4cats.pubsub.PublishCommands
import dev.profunktor.redis4cats.pubsub.data.Subscription

/** Wraps every command in [[PublishCommands]]. This is used for tracing, but can be used for something else as well. */
trait WrappedPublishCommands[F[_], S[_], K, V]
    extends PublishCommands[F, S, K, V]
    with CoreHelpers[K, V]
    with CommandWrapper[F] {
  private def Attributes = StreamsAttributes

  /** The underlying commands. */
  def cmd: PublishCommands[F, S, K, V]

  override def numPat: F[Long] =
    wrap("numPat")(cmd.numPat)

  override def numSub: F[List[Subscription[K]]] =
    wrap("numSub")(cmd.numSub)

  override def pubSubChannels: F[List[RedisChannel[K]]] =
    wrap("pubSubChannels")(cmd.pubSubChannels)

  override def pubSubShardChannels: F[List[RedisChannel[K]]] =
    wrap("pubSubShardChannels")(cmd.pubSubShardChannels)

  override def pubSubSubscriptions(channel: RedisChannel[K]): F[Option[Subscription[K]]] =
    wrap("pubSubSubscriptions", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(
      cmd.pubSubSubscriptions(channel)
    )

  override def pubSubSubscriptions(channels: List[RedisChannel[K]]): F[List[Subscription[K]]] =
    wrap("pubSubSubscriptions", keys2AsAttribute(channels.iterator.map(_.underlying), Attributes.Channels).toList)(
      cmd.pubSubSubscriptions(channels)
    )

  override def shardNumSub(channels: List[RedisChannel[K]]): F[List[Subscription[K]]] =
    wrap("shardNumSub", keys2AsAttribute(channels.iterator.map(_.underlying), Attributes.Channels).toList)(
      cmd.shardNumSub(channels)
    )

  override def publish(channel: RedisChannel[K]): S[V] => S[Unit] =
    cmd.publish(channel)

  override def publish(channel: RedisChannel[K], value: V): F[Unit] =
    wrap(
      "publish",
      keyAsAttribute(channel.underlying, Attributes.Channel).toList :::
        valueAsAttribute(value, Attributes.Value).toList
    )(cmd.publish(channel, value))
}
