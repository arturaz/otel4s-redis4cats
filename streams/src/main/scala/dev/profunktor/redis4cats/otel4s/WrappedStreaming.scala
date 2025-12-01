package dev.profunktor.redis4cats.otel4s

import dev.profunktor.redis4cats.RestartOnTimeout
import dev.profunktor.redis4cats.effects.{MessageId, StreamMessage, XReadOffsets}
import dev.profunktor.redis4cats.streams.{Streaming, data}
import dev.profunktor.redis4cats.otel4s.StreamArgAttributes.*
import dev.profunktor.redis4cats.otel4s.StreamAttributeKeys

import scala.concurrent.duration.Duration

/** Wraps every command in [[Streaming]]. This is used for tracing, but can be used for something else as well.
  */
trait WrappedStreaming[F[_], S[_], K, V] extends Streaming[F, S, K, V] {
  /** The underlying [[Streaming]]. */
  def cmd: Streaming[F, S, K, V]

  /** The wrapper to use. */
  def wrapper: CommandWrapper[F]

  val helpers: WrappingHelpers[K, V]

  import helpers.*

  override def append: S[data.XAddMessage[K, V]] => S[MessageId] =
    cmd.append

  override def append(message: data.XAddMessage[K, V]): F[MessageId] = {
    val data.XAddMessage(key, body, args) = message

    wrapper.wrap(
      "append",
      keyAsAttribute(key).toList :::
        kvsAsAttribute(body, StreamAttributeKeys.Body).toList :::
        xAddArgsAttributes(args)
    )(cmd.append(message))
  }

  override def read(
      streams: Set[XReadOffsets[K]],
      block: Option[Duration],
      count: Option[Long],
      restartOnTimeout: RestartOnTimeout
  ): S[StreamMessage[K, V]] =
    cmd.read(streams, block, count, restartOnTimeout)
}
