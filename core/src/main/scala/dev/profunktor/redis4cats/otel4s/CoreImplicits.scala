package dev.profunktor.redis4cats.otel4s

import cats.Show
import dev.profunktor.redis4cats.data
import io.lettuce.core.CompositeArgument
import io.lettuce.core.GeoArgs
import io.lettuce.core.ZAddArgs
import io.lettuce.core.ZAggregateArgs
import io.lettuce.core.ZStoreArgs
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.protocol.CommandArgs

import scala.util.control.NonFatal

trait CoreImplicits {
  trait ToString[F[_]] {
    def apply[A](fa: F[A], mapper: A => String): String
  }
  trait ToStrings[F[_]] {
    def apply[A](fa: F[A], mapper: A => String): Seq[String]
  }

  implicit val showZAggregateArgs: Show[ZAggregateArgs] = showForCompositeArgument
  implicit val showZAddArgs: Show[ZAddArgs] = showForCompositeArgument
  implicit val showZStoreArgs: Show[ZStoreArgs] = showForCompositeArgument
  implicit val showGeoArgs: Show[GeoArgs] = showForCompositeArgument
  implicit val showGeoArgsSort: Show[GeoArgs.Sort] = _.name()

  implicit val toStringsKeyScanCursor: ToStrings[data.KeyScanCursor] = new ToStrings[data.KeyScanCursor] {
    override def apply[A](fa: data.KeyScanCursor[A], mapper: A => String): Seq[String] =
      fa.keys.map(mapper)
  }

  private def codec: RedisCodec[String, String] = data.RedisCodec.Utf8.underlying

  def showForCompositeArgument[A <: CompositeArgument]: Show[A] = args => {
    try {
      val cmdArgs = new CommandArgs(codec)
      args.build(cmdArgs)
      cmdArgs.toCommandString()
    } catch {
      case NonFatal(e) => s"${args.getClass().getName()} failed: ${e.getMessage}"
    }
  }
}
object CoreImplicits extends CoreImplicits
