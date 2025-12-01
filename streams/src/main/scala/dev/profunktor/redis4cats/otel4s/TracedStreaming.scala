package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.syntax.all.*
import dev.profunktor.redis4cats.RestartOnTimeout
import dev.profunktor.redis4cats.effects.{StreamMessage, XReadOffsets}
import dev.profunktor.redis4cats.streams.Streaming
import org.typelevel.otel4s.trace.{SpanOps, Tracer, TracerProvider}

import scala.concurrent.duration.Duration

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
    new TracedStreamingImpl(config, cmd, config.asWrappingHelpers, config.asCommandWrapper)
  }
}
trait TracedStreaming[F[_], S[_], K, V] extends Streaming[F, S, K, V] with TracedModifiers[F, K, V] {
  override type Self <: TracedStreaming[F, S, K, V]

  /** As [[read]] but returns `SpanOps` for each message that introduces a span when it is used.
    *
    * You must use the `SpanOps` yourself to record the span.
    */
  def readWithTracedMessages(
      streams: Set[XReadOffsets[K]],
      block: Option[Duration],
      count: Option[Long],
      restartOnTimeout: RestartOnTimeout = RestartOnTimeout.always,
      spanName: StreamMessage[K, V] => String = _ => "message"
  ): S[(SpanOps[F], StreamMessage[K, V])]
}

// No stable ABI guaranteed
private class TracedStreamingImpl[F[_]: Tracer, S[_], K, V](
    config: TracedRedisConfig[F, K, V],
    val cmd: Streaming[F, S, K, V],
    val helpers: WrappingHelpers[K, V],
    val wrapper: CommandWrapper[F]
)(implicit SFunctor: Functor[S[*]])
    extends WrappedStreaming[F, S, K, V]
    with TracedStreaming[F, S, K, V] {
  override type Self = TracedStreaming[F, S, K, V]

  import helpers.*
  private def Attributes = StreamsAttributes

  /** Modifies the current [[WrappingHelpers]]. */
  override def withHelpers(f: WrappingHelpers[K, V] => WrappingHelpers[K, V]): TracedStreaming[F, S, K, V] =
    new TracedStreamingImpl(config, cmd, f(helpers), wrapper)

  /** Modifies the current [[CommandWrapper]]. */
  override def withWrapper(f: CommandWrapper[F] => CommandWrapper[F]): TracedStreaming[F, S, K, V] =
    new TracedStreamingImpl(config, cmd, helpers, f(wrapper))

  override def readWithTracedMessages(
      streams: Set[XReadOffsets[K]],
      block: Option[Duration],
      count: Option[Long],
      restartOnTimeout: RestartOnTimeout,
      spanName: StreamMessage[K, V] => String
  ): S[(SpanOps[F], StreamMessage[K, V])] =
    read(streams, block, count, restartOnTimeout).map { msg =>
      val StreamMessage(messageId, key, body) = msg

      val ops = config.spanOps(
        spanName(msg),
        Attributes.MessageId(messageId.value) :: keyAsAttribute(key).toList :::
          kvsAsAttribute(body, Attributes.Body).toList
      )

      (ops, msg)
    }
}
