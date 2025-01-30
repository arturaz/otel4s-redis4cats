package dev.profunktor.redis4cats.otel4s

import cats.Functor
import dev.profunktor.redis4cats.streams.data.StreamingOffset
import cats.Show
import cats.syntax.show.*

trait StreamsImplicits extends CoreImplicits {
  implicit val functorStreamingOffset: Functor[StreamingOffset] = new Functor[StreamingOffset] {
    override def map[A, B](fa: StreamingOffset[A])(f: A => B): StreamingOffset[B] =
      fa match {
        case StreamingOffset.All(key)            => StreamingOffset.All(f(key))
        case StreamingOffset.Latest(key)         => StreamingOffset.Latest(f(key))
        case StreamingOffset.Custom(key, offset) => StreamingOffset.Custom(f(key), offset)
      }
  }

  implicit val showStreamingOffset: Show[StreamingOffset[String]] = {
    case StreamingOffset.All(key)            => show"all($key)"
    case StreamingOffset.Latest(key)         => show"latest($key)"
    case StreamingOffset.Custom(key, offset) => show"custom(key=$key, offset=$offset)"
  }
}
object StreamsImplicits extends StreamsImplicits