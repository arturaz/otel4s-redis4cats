package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.all.*
import dev.profunktor.redis4cats.streams.Streaming
import dev.profunktor.redis4cats.streams.data
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.trace.SpanOps
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.trace.TracerProvider

import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

object TracedStreaming {

  /** Constructor for [[TracerProvider]]. */
  def apply[F[_]: Functor, S[_], K, V](
      cmd: Streaming[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit tracerProvider: TracerProvider[F], SFunctor: Functor[S[*]]): F[TracedStreaming[F, S, K, V]] = {
    tracerProvider
      .tracer("dev.profunktor.redis4cats.otel4s.TracedStreaming")
      .withVersion(buildinfo.BuildInfo.version)
      .get
      .map { implicit tracer =>
        fromTracer(cmd, config)
      }
  }

  /** Constructor for [[Tracer]]. */
  def fromTracer[F[_]: Tracer, S[_], K, V](
      cmd: Streaming[F, S, K, V],
      config: TracedRedisConfig[F, K, V]
  )(implicit SFunctor: Functor[S[*]]): TracedStreaming[F, S, K, V] = {
    new TracedStreamingImplementation(config, cmd)
  }
}
trait TracedStreaming[F[_], S[_], K, V] extends Streaming[F, S, K, V] {

  /** As [[read]] but returns `SpanOps` for each message that introduces a span when it is used.
    *
    * You must use the `SpanOps` yourself to record the span.
    */
  def readWithTracedMessages(
      keys: Set[K],
      chunkSize: Int,
      initialOffset: K => data.StreamingOffset[K],
      block: Option[Duration],
      count: Option[Long],
      restartOnTimeout: Option[FiniteDuration => Boolean],
      spanName: data.XReadMessage[K, V] => String = _ => "message"
  ): S[(SpanOps[F], data.XReadMessage[K, V])]
}

class TracedStreamingImplementation[F[_]: Tracer, S[_], K, V](
    config: TracedRedisConfig[F, K, V],
    val cmd: Streaming[F, S, K, V]
)(implicit SFunctor: Functor[S[*]])
    extends WrappedStreaming[F, S, K, V]
    with TracedStreaming[F, S, K, V] {
  import config.*
  private def Attributes = StreamsAttributes

  override def recordKey = config.recordKey
  override def recordValue = config.recordValue

  override def wrap[A](name: String, attributes: immutable.Iterable[Attribute[?]])(fa: F[A]): F[A] =
    config.span(name, attributes)(fa)

  override def readWithTracedMessages(
      keys: Set[K],
      chunkSize: Int,
      initialOffset: K => data.StreamingOffset[K],
      block: Option[Duration],
      count: Option[Long],
      restartOnTimeout: Option[FiniteDuration => Boolean],
      spanName: data.XReadMessage[K, V] => String
  ): S[(SpanOps[F], data.XReadMessage[K, V])] =
    read(keys, chunkSize, initialOffset, block, count, restartOnTimeout).map { msg =>
      val data.XReadMessage(messageId, key, body) = msg

      val ops = spanOps(
        spanName(msg),
        Attributes.MessageId(messageId.value) :: keyAsAttribute(key).toList :::
          kvsAsAttribute(body, Attributes.Body).toList
      )

      (ops, msg)
    }
}
