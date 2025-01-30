package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.all.*
import dev.profunktor.redis4cats.data.*
import dev.profunktor.redis4cats.pubsub.PubSubCommands
import dev.profunktor.redis4cats.pubsub.data.*
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.trace.TracerProvider

object TracedPubSubCommands {

  /** Constructor for [[TracerProvider]]. */
  def apply[F[_]: Functor, S[_[_], _], K, V](
      cmd: PubSubCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit tracerProvider: TracerProvider[F], SFunctor: Functor[S[F, *]]): F[TracedPubSubCommands[F, S, K, V]] = {
    tracerProvider
      .tracer("dev.profunktor.redis4cats.otel4s.TracedPubSubCommands")
      .withVersion(buildinfo.BuildInfo.version)
      .get
      .map { implicit tracer =>
        fromTracer(cmd, config)
      }
  }

  /** Constructor for [[Tracer]]. */
  def fromTracer[F[_]: Tracer, S[_[_], _], K, V](
      cmd: PubSubCommands[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit SFunctor: Functor[S[F, *]]): TracedPubSubCommands[F, S, K, V] = {
    val pub = new TracedPublishCommandsImplementation(config, cmd)
    val sub = new TracedSubscribeCommandsImplementation(config, cmd)
    new TracedPubSubCommandsImplementation(pub, sub)
  }
}
trait TracedPubSubCommands[F[_], S[_[_], _], K, V]
    extends TracedPublishCommands[F, S, K, V]
    with TracedSubscribeCommands[F, S, K, V]
    with PubSubCommands[F, S, K, V]
class TracedPubSubCommandsImplementation[F[_], S[_[_], _], K, V](
    pub: TracedPublishCommands[F, S, K, V],
    sub: TracedSubscribeCommands[F, S, K, V]
) extends TracedPubSubCommands[F, S, K, V]
    with TracedSubscribeCommands[F, S, K, V] {

  override def numPat: F[Long] = pub.numPat

  override def numSub: F[List[Subscription[K]]] = pub.numSub

  override def pubSubChannels: F[List[RedisChannel[K]]] = pub.pubSubChannels

  override def pubSubShardChannels: F[List[RedisChannel[K]]] = pub.pubSubShardChannels

  override def pubSubSubscriptions(channel: RedisChannel[K]): F[Option[Subscription[K]]] =
    pub.pubSubSubscriptions(channel)

  override def pubSubSubscriptions(channels: List[RedisChannel[K]]): F[List[Subscription[K]]] =
    pub.pubSubSubscriptions(channels)

  override def shardNumSub(channels: List[RedisChannel[K]]): F[List[Subscription[K]]] = pub.shardNumSub(channels)

  override def publish(channel: RedisChannel[K]) = pub.publish(channel)

  override def publish(channel: RedisChannel[K], value: V) = pub.publish(channel, value)

  override def subscribe(channel: RedisChannel[K]) = sub.subscribe(channel)

  override def unsubscribe(channel: RedisChannel[K]): F[Unit] = sub.unsubscribe(channel)

  override def psubscribe(channel: RedisPattern[K]) = sub.psubscribe(channel)

  override def punsubscribe(channel: RedisPattern[K]): F[Unit] = sub.punsubscribe(channel)

  override def subscribeWithTracedEvents(channel: RedisChannel[K], eventName: V => String) =
    sub.subscribeWithTracedEvents(channel, eventName)

  override def psubscribeWithTracedEvents(
      channel: RedisPattern[K],
      eventName: RedisPatternEvent[K, V] => String
  ) = sub.psubscribeWithTracedEvents(channel, eventName)

}
