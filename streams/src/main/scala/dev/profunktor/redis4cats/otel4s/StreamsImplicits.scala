package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.Show
import cats.syntax.show.*
import dev.profunktor.redis4cats.effects.XReadOffsets

trait StreamsImplicits extends CoreImplicits {
  implicit val functorStreamingOffset: Functor[XReadOffsets] = new Functor[XReadOffsets] {
    override def map[A, B](fa: XReadOffsets[A])(f: A => B): XReadOffsets[B] =
      fa match {
        case XReadOffsets.All(key)            => XReadOffsets.All(f(key))
        case XReadOffsets.Latest(key)         => XReadOffsets.Latest(f(key))
        case XReadOffsets.Custom(key, offset) => XReadOffsets.Custom(f(key), offset)
      }
  }

  implicit val showStreamingOffset: Show[XReadOffsets[String]] = {
    case XReadOffsets.All(key)            => show"all($key)"
    case XReadOffsets.Latest(key)         => show"latest($key)"
    case XReadOffsets.Custom(key, offset) => show"custom(key=$key, offset=$offset)"
  }
}
object StreamsImplicits extends StreamsImplicits
