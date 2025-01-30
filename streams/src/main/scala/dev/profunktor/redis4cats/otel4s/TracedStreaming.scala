package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.effect.kernel.Resource
import cats.syntax.all.*
import dev.profunktor.redis4cats.streams.Streaming
import dev.profunktor.redis4cats.streams.data
import org.typelevel.otel4s.trace.SpanOps
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.trace.TracerProvider

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

object TracedStreaming {

  /** Constructor for [[TracerProvider]]. */
  def apply[F[_]: Functor, S[_[_], _], K, V](
      cmd: Streaming[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit tracerProvider: TracerProvider[F], SFunctor: Functor[S[F, *]]): F[TracedStreaming[F, S, K, V]] = {
    tracerProvider
      .tracer("dev.profunktor.redis4cats.otel4s.TracedStreaming")
      .withVersion(buildinfo.BuildInfo.version)
      .get
      .map { implicit tracer =>
        fromTracer(cmd, config)
      }
  }

  /** Constructor for [[Tracer]]. */
  def fromTracer[F[_]: Tracer, S[_[_], _], K, V](
      cmd: Streaming[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit SFunctor: Functor[S[F, *]]): TracedStreaming[F, S, K, V] = {
    new TracedStreamingImplementation(config, cmd)
  }
}
trait TracedStreaming[F[_], S[_[_], _], K, V] extends Streaming[F, S, K, V] {

  /** As [[read]] but returns a resource for each message that introduces a span when it is used. */
  def readWithTracedMessages(
      keys: Set[K],
      chunkSize: Int,
      initialOffset: K => data.StreamingOffset[K],
      block: Option[Duration],
      count: Option[Long],
      restartOnTimeout: Option[FiniteDuration => Boolean],
      spanName: data.XReadMessage[K, V] => String = _ => "message"
  ): S[F, Resource[F, (SpanOps.Res[F], data.XReadMessage[K, V])]]
}

private class TracedStreamingImplementation[F[_]: Tracer, S[_[_], _], K, V](
    config: TracedRedisConfig[F, K, V],
    cmd: Streaming[F, S, K, V]
)(implicit SFunctor: Functor[S[F, *]])
    extends TracedStreaming[F, S, K, V]
    with CoreHelpers[K, V] {
  import config.*
  private def Attributes = StreamsAttributes

  override def recordKey = config.recordKey
  override def recordValue = config.recordValue

  override def append: S[F, data.XAddMessage[K, V]] => S[F, data.MessageId] = cmd.append

  override def append(message: data.XAddMessage[K, V]): F[data.MessageId] = {
    val data.XAddMessage(key, body, approxMaxlen, minId) = message

    span(
      "append",
      keyAsAttribute(key).toList ::: kvsAsAttribute(body, Attributes.Body).toList :::
        approxMaxlen.map(Attributes.ApproxMaxlen(_)).toList ::: minId.map(Attributes.MinId(_)).toList
    )(cmd.append(message))
  }

  override def read(
      keys: Set[K],
      chunkSize: Int,
      initialOffset: K => data.StreamingOffset[K],
      block: Option[Duration],
      count: Option[Long],
      restartOnTimeout: Option[FiniteDuration => Boolean]
  ) = cmd.read(keys, chunkSize, initialOffset, block, count, restartOnTimeout)

  override def readWithTracedMessages(
      keys: Set[K],
      chunkSize: Int,
      initialOffset: K => data.StreamingOffset[K],
      block: Option[Duration],
      count: Option[Long],
      restartOnTimeout: Option[FiniteDuration => Boolean],
      spanName: data.XReadMessage[K, V] => String
  ): S[F, Resource[F, (SpanOps.Res[F], data.XReadMessage[K, V])]] =
    read(keys, chunkSize, initialOffset, block, count, restartOnTimeout).map { msg =>
      val data.XReadMessage(messageId, key, body) = msg

      spanBuilder(
        spanName(msg),
        Attributes.MessageId(messageId.value) :: keyAsAttribute(key).toList :::
          kvsAsAttribute(body, Attributes.Body).toList
      ).resource.map((_, msg))
    }
}
