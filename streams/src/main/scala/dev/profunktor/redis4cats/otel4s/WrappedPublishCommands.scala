package dev.profunktor.redis4cats.otel4s

import dev.profunktor.redis4cats.data.RedisChannel
import dev.profunktor.redis4cats.pubsub.PublishCommands
import dev.profunktor.redis4cats.pubsub.data.Subscription

/** Wraps every command in [[PublishCommands]]. This is used for tracing, but can be used for something else as well. */
trait WrappedPublishCommands[F[_], S[_], K, V] extends PublishCommands[F, S, K, V] {
  private def Attributes = StreamsAttributes

  /** The underlying commands. */
  def cmd: PublishCommands[F, S, K, V]

  /** The wrapper to use. */
  def wrapper: CommandWrapper[F]

  val helpers: WrappingHelpers[K, V]

  import helpers.*

  override def numPat: F[Long] =
    wrapper.wrap("numPat")(cmd.numPat)

  override def numSub: F[List[Subscription[K]]] =
    wrapper.wrap("numSub")(cmd.numSub)

  override def pubSubChannels: F[List[RedisChannel[K]]] =
    wrapper.wrap("pubSubChannels")(cmd.pubSubChannels)

  override def pubSubShardChannels: F[List[RedisChannel[K]]] =
    wrapper.wrap("pubSubShardChannels")(cmd.pubSubShardChannels)

  override def pubSubSubscriptions(channel: RedisChannel[K]): F[Option[Subscription[K]]] =
    wrapper.wrap("pubSubSubscriptions", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(
      cmd.pubSubSubscriptions(channel)
    )

  override def pubSubSubscriptions(channels: List[RedisChannel[K]]): F[List[Subscription[K]]] =
    wrapper.wrap(
      "pubSubSubscriptions",
      keys2AsAttribute(channels.iterator.map(_.underlying), Attributes.Channels).toList
    )(
      cmd.pubSubSubscriptions(channels)
    )

  override def shardNumSub(channels: List[RedisChannel[K]]): F[List[Subscription[K]]] =
    wrapper.wrap("shardNumSub", keys2AsAttribute(channels.iterator.map(_.underlying), Attributes.Channels).toList)(
      cmd.shardNumSub(channels)
    )

  override def publish(channel: RedisChannel[K]): S[V] => S[Unit] =
    cmd.publish(channel)

  override def publish(channel: RedisChannel[K], value: V): F[Unit] =
    wrapper.wrap(
      "publish",
      keyAsAttribute(channel.underlying, Attributes.Channel).toList :::
        valueAsAttribute(value, Attributes.Value).toList
    )(cmd.publish(channel, value))
}
