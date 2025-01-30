package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.Show
import cats.syntax.contravariant.*
import cats.syntax.show.*
import dev.profunktor.redis4cats.algebra.BitCommandOperation
import dev.profunktor.redis4cats.effects
import io.lettuce.core

trait EffectsImplicits extends CoreImplicits {
  implicit val showBitCommandOperation: Show[BitCommandOperation] = _.toString
  implicit val showKeyScanArgs: Show[effects.KeyScanArgs] =
    showForCompositeArgument[core.KeyScanArgs].contramap(_.underlying)
  implicit val showScanArgs: Show[effects.ScanArgs] = showForCompositeArgument[core.ScanArgs].contramap(_.underlying)
  implicit val showExpireExistenceArg: Show[effects.ExpireExistenceArg] = _.toString
  implicit val showFlushMode: Show[effects.FlushMode] = _.toString
  implicit val showFunctionRestoreMode: Show[effects.FunctionRestoreMode] = _.toString
  implicit val showGeoLocation: Show[effects.GeoLocation[String]] = v => {
    val effects.GeoLocation(lon, lat, value) = v
    show"lon=${lon.value} lat=${lat.value} value=$value"
  }

  implicit val functorZRange: Functor[effects.ZRange] = new Functor[effects.ZRange] {
    override def map[A, B](fa: effects.ZRange[A])(f: A => B): effects.ZRange[B] =
      effects.ZRange(f(fa.start), f(fa.end))
  }

  implicit val functorGeoRadiusKeyStorage: Functor[effects.GeoRadiusKeyStorage] =
    new Functor[effects.GeoRadiusKeyStorage] {
      override def map[A, B](fa: effects.GeoRadiusKeyStorage[A])(f: A => B): effects.GeoRadiusKeyStorage[B] =
        effects.GeoRadiusKeyStorage(f(fa.key), fa.count, fa.sort)
    }

  implicit val functorGeoRadiusDistStorage: Functor[effects.GeoRadiusDistStorage] =
    new Functor[effects.GeoRadiusDistStorage] {
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
}
object EffectsImplicits extends EffectsImplicits
