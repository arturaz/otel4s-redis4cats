package dev.profunktor.redis4cats.otel4s

import dev.profunktor.redis4cats.data.{RedisChannel, RedisPattern, RedisPatternEvent}
import dev.profunktor.redis4cats.pubsub.SubscribeCommands

/** Wraps every command in [[SubscribeCommands]]. This is used for tracing, but can be used for something else as well.
  */
trait WrappedSubscribeCommands[F[_], S[_], K, V] extends SubscribeCommands[F, S, K, V] {
  private def Attributes = StreamsAttributes

  /** The underlying commands. */
  def cmd: SubscribeCommands[F, S, K, V]

  /** The wrapper to use. */
  def wrapper: CommandWrapper[F]

  val helpers: WrappingHelpers[K, V]

  import helpers.*

  override def subscribe(channel: RedisChannel[K]): S[V] =
    cmd.subscribe(channel)

  override def unsubscribe(channel: RedisChannel[K]): F[Unit] =
    wrapper.wrap("unsubscribe", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(cmd.unsubscribe(channel))

  override def psubscribe(channel: RedisPattern[K]): S[RedisPatternEvent[K, V]] =
    cmd.psubscribe(channel)

  override def punsubscribe(channel: RedisPattern[K]): F[Unit] =
    wrapper.wrap("punsubscribe", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(
      cmd.punsubscribe(channel)
    )

  override def internalChannelSubscriptions: F[Map[RedisChannel[K], Long]] =
    wrapper.wrap("internalChannelSubscriptions")(cmd.internalChannelSubscriptions)

  override def internalPatternSubscriptions: F[Map[RedisPattern[K], Long]] =
    wrapper.wrap("internalPatternSubscriptions")(cmd.internalPatternSubscriptions)
}
