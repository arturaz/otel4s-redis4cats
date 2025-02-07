package dev.profunktor.redis4cats.otel4s

import dev.profunktor.redis4cats.streams.{Streaming, data}

import scala.concurrent.duration.{Duration, FiniteDuration}

/** Wraps every command in [[Streaming]]. This is used for tracing, but can be used for something else as well.
  */
trait WrappedStreaming[F[_], S[_], K, V] extends Streaming[F, S, K, V] {
  private def Attributes = StreamsAttributes

  /** The underlying [[Streaming]]. */
  def cmd: Streaming[F, S, K, V]

  /** The wrapper to use. */
  def wrapper: CommandWrapper[F]

  val helpers: WrappingHelpers[K, V]

  import helpers.*

  override def append: S[data.XAddMessage[K, V]] => S[data.MessageId] =
    cmd.append

  override def append(message: data.XAddMessage[K, V]): F[data.MessageId] = {
    val data.XAddMessage(key, body, approxMaxlen, minId) = message

    wrapper.wrap(
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
}
