package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.Show
import cats.syntax.contravariant.*
import cats.syntax.show.*
import dev.profunktor.redis4cats.data
import dev.profunktor.redis4cats.effects
import io.lettuce.core.CompositeArgument
import io.lettuce.core.GeoArgs
import io.lettuce.core.ZAddArgs
import io.lettuce.core.ZAggregateArgs
import io.lettuce.core.ZStoreArgs
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.protocol.CommandArgs

import scala.util.control.NonFatal

import Otel4sRedisAttributes.*
import dev.profunktor.redis4cats.algebra.BitCommandOperation
import io.lettuce.core.KeyScanArgs
import io.lettuce.core

object Implicits {
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
  implicit val showBitCommandOperation: Show[BitCommandOperation] = _.toString
  implicit val showKeyScanArgs: Show[effects.KeyScanArgs] = showForCompositeArgument[KeyScanArgs].contramap(_.underlying)
  implicit val showScanArgs: Show[effects.ScanArgs] = showForCompositeArgument[core.ScanArgs].contramap(_.underlying)
  implicit val showExpireExistenceArg: Show[effects.ExpireExistenceArg] = _.toString
  implicit val showFlushMode: Show[effects.FlushMode] = _.toString
  implicit val showFunctionRestoreMode: Show[effects.FunctionRestoreMode] = _.toString
  implicit val showGeoLocation: Show[effects.GeoLocation[String]] = v => {
    val effects.GeoLocation(lon, lat, value) = v
    show"lon=${lon.value} lat=${lat.value} value=$value"
  }

  implicit val functorGeoRadiusKeyStorage: Functor[effects.GeoRadiusKeyStorage] = new Functor[effects.GeoRadiusKeyStorage] {
    override def map[A, B](fa: effects.GeoRadiusKeyStorage[A])(f: A => B): effects.GeoRadiusKeyStorage[B] =
      effects.GeoRadiusKeyStorage(f(fa.key), fa.count, fa.sort)
  }

  implicit val functorGeoRadiusDistStorage: Functor[effects.GeoRadiusDistStorage] = new Functor[effects.GeoRadiusDistStorage] {
    override def map[A, B](fa: effects.GeoRadiusDistStorage[A])(f: A => B): effects.GeoRadiusDistStorage[B] =
      effects.GeoRadiusDistStorage(f(fa.key), fa.count, fa.sort)
  }

  implicit val functorGeoLocation: Functor[effects.GeoLocation] = new Functor[effects.GeoLocation] {
    override def map[A, B](fa: effects.GeoLocation[A])(f: A => B): effects.GeoLocation[B] =
      effects.GeoLocation(fa.lon, fa.lat, f(fa.value))
  }

  implicit val toStringZRange: ToString[effects.ZRange] = new ToString[effects.ZRange] {
    override def apply[A](fa: effects.ZRange[A], mapper: A => String): String =
      s"${mapper(fa.start)} - ${mapper(fa.end)}"
  }
  implicit def keyForZRange[A]: KeyFor[effects.ZRange[A]] { type Out = String } = KeyFor(Range)

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
