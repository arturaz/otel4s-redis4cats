package dev.profunktor.redis4cats.otel4s

import org.typelevel.otel4s.AttributeKey
import scala.concurrent.duration.FiniteDuration
import org.typelevel.otel4s.Attribute
import dev.profunktor.redis4cats.effects
import scala.concurrent.duration.Duration
import io.lettuce.core.ZAggregateArgs
import cats.syntax.show.*
import io.lettuce.core
import dev.profunktor.redis4cats.algebra.BitCommandOperation
import java.time.Instant

object Otel4sRedisAttributes {
  import Implicits.*

  trait KeyFor[A] {
    type Out

    def key: AttributeKey[Out]
  }
  object KeyFor {
    def apply[A, Out_](key: AttributeKey[Out_]): KeyFor[A] { type Out = Out_ } = {
      val k = key
      new KeyFor[A] {
        type Out = Out_
        def key = k
      }
    }
  }

  val Key: AttributeKey[String] = AttributeKey.string("db.redis.key")
  val Keys: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.keys")
  val KeyValuePairs: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.keyValuePairs")
  val Value: AttributeKey[String] = AttributeKey.string("db.redis.value")
  val ValueLong: AttributeKey[Long] = AttributeKey.long("db.redis.value")
  val Values: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.values")
  val GetExArg: AttributeKey[String] = AttributeKey.string("db.redis.getExArg")
  val From: AttributeKey[String] = AttributeKey.string("db.redis.from")
  val To: AttributeKey[String] = AttributeKey.string("db.redis.to")
  val Start: AttributeKey[Long] = AttributeKey.long("db.redis.start")
  val Stop: AttributeKey[Long] = AttributeKey.long("db.redis.stop")
  val End: AttributeKey[Long] = AttributeKey.long("db.redis.end")
  val SetArgs: AttributeKey[String] = AttributeKey.string("db.redis.setArgs")
  val State: AttributeKey[Boolean] = AttributeKey.boolean("db.redis.state")

  /** Expiration time in milliseconds. */
  val ExpiresIn: AttributeKey[Long] = AttributeKey.long("db.redis.expiresIn")
  def expiresIn(fd: FiniteDuration): Attribute[Long] = ExpiresIn(fd.toMillis)
  val ExpireExistenceArg: AttributeKey[String] = AttributeKey.string("db.redis.expireExistenceArg")
  def expireExistenceArg(arg: effects.ExpireExistenceArg): Attribute[String] = ExpireExistenceArg(arg.show)

  val Offset: AttributeKey[Long] = AttributeKey.long("db.redis.offset")
  val Amount: AttributeKey[Long] = AttributeKey.long("db.redis.amount")
  val AmountDouble: AttributeKey[Double] = AttributeKey.double("db.redis.amount")
  val Field: AttributeKey[String] = AttributeKey.string("db.redis.field")
  val Fields: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.fields")
  val FieldValues: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.fieldValues")
  val Count: AttributeKey[Long] = AttributeKey.long("db.redis.count")
  val Source: AttributeKey[String] = AttributeKey.string("db.redis.source")
  val Destination: AttributeKey[String] = AttributeKey.string("db.redis.destination")
  val Index: AttributeKey[Long] = AttributeKey.long("db.redis.index")
  val Pivot: AttributeKey[String] = AttributeKey.string("db.redis.pivot")

  val Range: AttributeKey[String] = AttributeKey.string("db.redis.range")
  def range[A](range: effects.ZRange[A]): Attribute[String] = Range(range.toString)

  val RangeLimitOffset: AttributeKey[Long] = AttributeKey.long("db.redis.rangeLimit.offset")
  val RangeLimitCount: AttributeKey[Long] = AttributeKey.long("db.redis.rangeLimit.count")
  def rangeLimit(limit: effects.RangeLimit): List[Attribute[Long]] = {
    val effects.RangeLimit(offset, count) = limit
    RangeLimitOffset(offset) :: RangeLimitCount(count) :: Nil
  }
  def rangeLimit(limit: Option[effects.RangeLimit]): List[Attribute[Long]] = limit match {
    case None        => Nil
    case Some(limit) => rangeLimit(limit)
  }

  def durationAsLong(duration: Duration): Long = duration match {
    case fd: FiniteDuration => fd.toMillis
    case _: Duration        => -1
  }

  val Timeout: AttributeKey[Long] = AttributeKey.long("db.redis.timeout")
  def timeout(duration: Duration): Attribute[Long] = Timeout(durationAsLong(duration))

  val AggregateArgs: AttributeKey[String] = AttributeKey.string("db.redis.aggregateArgs")
  def aggregateArgs(args: ZAggregateArgs): Attribute[String] = AggregateArgs(args.show)

  val ScoreWithValueScore: AttributeKey[Double] = AttributeKey.double("db.redis.scoreWithValue.score")
  val ScoreWithValueValue: AttributeKey[String] = AttributeKey.string("db.redis.scoreWithValue.value")
  def scoreWithValue(scoreWithValue: effects.ScoreWithValue[String]): List[Attribute[_]] =
    ScoreWithValueScore(scoreWithValue.score.value) :: ScoreWithValueValue(scoreWithValue.value) :: Nil
  def scoreWithValue[A](mapper: Option[A => String], scoreWithValue: effects.ScoreWithValue[A]): List[Attribute[_]] =
    mapper match {
      case None => Nil
      case Some(mapper) =>
        val string = effects.ScoreWithValue(scoreWithValue.score, mapper(scoreWithValue.value))
        this.scoreWithValue(string)
    }

  def scoreWithValueAsString(scoreWithValue: effects.ScoreWithValue[String]): String =
    show"score=${scoreWithValue.score.value} value=${scoreWithValue.value}"

  def scoresWithValue[A](
      mapper: Option[A => String],
      scoresWithValue: Iterable[effects.ScoreWithValue[A]],
      attr: AttributeKey[Seq[String]] = Values
  ): Option[Attribute[Seq[String]]] = mapper.map { mapper =>
    attr(
      scoresWithValue.iterator
        .map(v => scoreWithValueAsString(effects.ScoreWithValue(v.score, mapper(v.value))))
        .toSeq
    )
  }

  val ZAddArgs: AttributeKey[String] = AttributeKey.string("db.redis.zAddArgs")
  def zAddArgs(args: core.ZAddArgs): Attribute[String] = ZAddArgs(args.show)

  val ZStoreArgs: AttributeKey[String] = AttributeKey.string("db.redis.zStoreArgs")
  def zStoreArgs(args: core.ZStoreArgs): Attribute[String] = ZStoreArgs(args.show)

  val Member: AttributeKey[String] = AttributeKey.string("db.redis.member")

  val GeoUnit: AttributeKey[String] = AttributeKey.string("db.redis.geoUnit")
  def geoUnit(unit: io.lettuce.core.GeoArgs.Unit): Attribute[String] = GeoUnit(unit.name())

  val GeoValues: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.geoValues")

  val GeoRadiusLatitude: AttributeKey[Double] = AttributeKey.double("db.redis.geoRadius.latitude")
  val GeoRadiusLongitude: AttributeKey[Double] = AttributeKey.double("db.redis.geoRadius.longitude")
  val GeoRadiusDistance: AttributeKey[Double] = AttributeKey.double("db.redis.geoRadius.distance")
  def geoRadius(radius: effects.GeoRadius): List[Attribute[_]] = {
    val effects.GeoRadius(latitude, longitude, distance) = radius
    GeoRadiusLatitude(latitude.value) :: GeoRadiusLongitude(longitude.value) :: GeoRadiusDistance(
      distance.value
    ) :: Nil
  }

  val GeoArgs: AttributeKey[String] = AttributeKey.string("db.redis.geoArgs")
  def geoArgs(args: core.GeoArgs): Attribute[String] = GeoArgs(args.show)

  val GeoRadiusKeyStorageKey: AttributeKey[String] = AttributeKey.string("db.redis.geoRadiusKeyStorage.key")
  val GeoRadiusKeyStorageCount: AttributeKey[Long] = AttributeKey.long("db.redis.geoRadiusKeyStorage.count")
  val GeoRadiusKeyStorageSort: AttributeKey[String] = AttributeKey.string("db.redis.geoRadiusKeyStorage.sort")
  def geoRadiusKeyStorage(s: effects.GeoRadiusKeyStorage[String]): List[Attribute[_]] = {
    val effects.GeoRadiusKeyStorage(key, count, sort) = s
    GeoRadiusKeyStorageKey(key) :: GeoRadiusKeyStorageCount(count) :: GeoRadiusKeyStorageSort(sort.show) :: Nil
  }

  val GeoRadiusDistStorageKey: AttributeKey[String] = AttributeKey.string("db.redis.geoRadiusDistStorage.key")
  val GeoRadiusDistStorageCount: AttributeKey[Long] = AttributeKey.long("db.redis.geoRadiusDistStorage.count")
  val GeoRadiusDistStorageSort: AttributeKey[String] = AttributeKey.string("db.redis.geoRadiusDistStorage.sort")
  def geoRadiusDistStorage(s: effects.GeoRadiusDistStorage[String]): List[Attribute[_]] = {
    val effects.GeoRadiusDistStorage(key, count, sort) = s
    GeoRadiusDistStorageKey(key) :: GeoRadiusDistStorageCount(count) :: GeoRadiusDistStorageSort(sort.show) :: Nil
  }

  val Distance: AttributeKey[Double] = AttributeKey.double("db.redis.distance")
  def distance(dist: effects.Distance): Attribute[Double] = Distance(dist.value)

  val Operations: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.operations")
  def operations(ops: BitCommandOperation*): Attribute[Seq[String]] = Operations(ops.map(_.show))

  val OutputKey: AttributeKey[String] = AttributeKey.string("db.redis.outputKey")
  val InputKeys: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.inputKeys")

  val Cursor: AttributeKey[Long] = AttributeKey.long("db.redis.cursor")
  val CursorAsKeyScanCursor: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.cursor")
  val KeyScanCursor: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.keyScanCursor")
  val KeyScanArgs: AttributeKey[String] = AttributeKey.string("db.redis.keyScanArgs")
  val ScanArgs: AttributeKey[String] = AttributeKey.string("db.redis.scanArgs")
  val Previous: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.previous")

  val RestoreArgsTtl: AttributeKey[Long] = AttributeKey.long("db.redis.restoreArgs.ttl")
  val RestoreArgsReplace: AttributeKey[Boolean] = AttributeKey.boolean("db.redis.restoreArgs.replace")
  val RestoreArgsAbsTtl: AttributeKey[Boolean] = AttributeKey.boolean("db.redis.restoreArgs.absTtl")
  val RestoreArgsIdleTime: AttributeKey[Long] = AttributeKey.long("db.redis.restoreArgs.idleTime")
  def restoreArgs(args: effects.RestoreArgs): List[Attribute[_]] = {
    val effects.RestoreArgs(ttl, replace, absTtl, idleTime) = args

    ttl.map(RestoreArgsTtl(_)).toList :::
      replace.map(RestoreArgsReplace(_)).toList :::
      absTtl.map(RestoreArgsAbsTtl(_)).toList :::
      idleTime.map(RestoreArgsIdleTime(_)).toList
  }

  val Section: AttributeKey[String] = AttributeKey.string("db.redis.section")
  val EffectCount: AttributeKey[Long] = AttributeKey.long("db.redis.effectCount")
  val FlushMode: AttributeKey[String] = AttributeKey.string("db.redis.flushMode")
  val Version: AttributeKey[String] = AttributeKey.string("db.redis.version")
  val Name: AttributeKey[String] = AttributeKey.string("db.redis.name")
  val Digest: AttributeKey[String] = AttributeKey.string("db.redis.digest")
  val Digests: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.digests")
  val Function: AttributeKey[String] = AttributeKey.string("db.redis.function")
  val Replace: AttributeKey[Boolean] = AttributeKey.boolean("db.redis.replace")
  val FunctionRestoreMode: AttributeKey[String] = AttributeKey.string("db.redis.functionRestoreMode")
  val LibraryName: AttributeKey[String] = AttributeKey.string("db.redis.libraryName")

  val CopyArgsDestinationDb: AttributeKey[Long] = AttributeKey.long("db.redis.copyArgs.destinationDb")
  val CopyArgsReplace: AttributeKey[Boolean] = AttributeKey.boolean("db.redis.copyArgs.replace")
  def copyArgs(args: effects.CopyArgs): List[Attribute[_]] = {
    val effects.CopyArgs(destinationDb, replace) = args
    destinationDb.map(CopyArgsDestinationDb(_)).toList ::: replace.map(CopyArgsReplace(_)).toList
  }

  /** Unix epoc timestamp in milliseconds. */
  val At: AttributeKey[Long] = AttributeKey.long("db.redis.at")
  def at(at: Instant): Attribute[Long] = At(at.toEpochMilli)
}
