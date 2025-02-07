package dev.profunktor.redis4cats.otel4s

import dev.profunktor.redis4cats.data.{RedisChannel, RedisPattern, RedisPatternEvent}
import dev.profunktor.redis4cats.pubsub.SubscribeCommands

/** Wraps every command in [[SubscribeCommands]]. This is used for tracing, but can be used for something else as well.
  */
trait WrappedSubscribeCommands[F[_], S[_], K, V]
    extends SubscribeCommands[F, S, K, V]
    with CoreHelpers[K, V]
    with CommandWrapper[F] {
  private def Attributes = StreamsAttributes

  /** The underlying commands. */
  def cmd: SubscribeCommands[F, S, K, V]

  override def subscribe(channel: RedisChannel[K]): S[V] =
    cmd.subscribe(channel)

  override def unsubscribe(channel: RedisChannel[K]): F[Unit] =
    wrap("unsubscribe", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(cmd.unsubscribe(channel))

  override def psubscribe(channel: RedisPattern[K]): S[RedisPatternEvent[K, V]] =
    cmd.psubscribe(channel)

  override def punsubscribe(channel: RedisPattern[K]): F[Unit] =
    wrap("punsubscribe", keyAsAttribute(channel.underlying, Attributes.Channel).toList)(cmd.punsubscribe(channel))
}
