package dev.profunktor.redis4cats.otel4s

import cats.syntax.show.*
import dev.profunktor.redis4cats.algebra.BitCommandOperation
import dev.profunktor.redis4cats.effects
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.AttributeKey

trait EffectsAttributes extends CoreAttributes {
  import EffectsImplicits.*

  val ExpireExistenceArg: AttributeKey[String] = AttributeKey.string("db.redis.expireExistenceArg")
  def expireExistenceArg(arg: effects.ExpireExistenceArg): Attribute[String] = ExpireExistenceArg(arg.show)

  val RangeStart: AttributeKey[String] = AttributeKey.string("db.redis.range.start")
  val RangeEnd: AttributeKey[String] = AttributeKey.string("db.redis.range.end")
  def range(range: effects.ZRange[String]): List[Attribute[String]] =
    RangeStart(range.start) :: RangeEnd(range.end) :: Nil

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

  val CopyArgsDestinationDb: AttributeKey[Long] = AttributeKey.long("db.redis.copyArgs.destinationDb")
  val CopyArgsReplace: AttributeKey[Boolean] = AttributeKey.boolean("db.redis.copyArgs.replace")
  def copyArgs(args: effects.CopyArgs): List[Attribute[_]] = {
    val effects.CopyArgs(destinationDb, replace) = args
    destinationDb.map(CopyArgsDestinationDb(_)).toList ::: replace.map(CopyArgsReplace(_)).toList
  }

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

  val GeoRadiusLatitude: AttributeKey[Double] = AttributeKey.double("db.redis.geoRadius.latitude")
  val GeoRadiusLongitude: AttributeKey[Double] = AttributeKey.double("db.redis.geoRadius.longitude")
  val GeoRadiusDistance: AttributeKey[Double] = AttributeKey.double("db.redis.geoRadius.distance")
  def geoRadius(radius: effects.GeoRadius): List[Attribute[_]] = {
    val effects.GeoRadius(latitude, longitude, distance) = radius
    GeoRadiusLatitude(latitude.value) :: GeoRadiusLongitude(longitude.value) :: GeoRadiusDistance(
      distance.value
    ) :: Nil
  }

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
}
object EffectsAttributes extends EffectsAttributes