package dev.profunktor.redis4cats.otel4s

import cats.syntax.show.*
import io.lettuce.core
import io.lettuce.core.ZAggregateArgs
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.AttributeKey

import java.time.Instant
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

trait CoreAttributes {
  import CoreImplicits.*

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

  def durationAsLong(duration: Duration): Long = duration match {
    case fd: FiniteDuration => fd.toMillis
    case _: Duration        => -1
  }

  val Timeout: AttributeKey[Long] = AttributeKey.long("db.redis.timeout")
  def timeout(duration: Duration): Attribute[Long] = Timeout(durationAsLong(duration))

  val AggregateArgs: AttributeKey[String] = AttributeKey.string("db.redis.aggregateArgs")
  def aggregateArgs(args: ZAggregateArgs): Attribute[String] = AggregateArgs(args.show)

  val ZAddArgs: AttributeKey[String] = AttributeKey.string("db.redis.zAddArgs")
  def zAddArgs(args: core.ZAddArgs): Attribute[String] = ZAddArgs(args.show)

  val ZStoreArgs: AttributeKey[String] = AttributeKey.string("db.redis.zStoreArgs")
  def zStoreArgs(args: core.ZStoreArgs): Attribute[String] = ZStoreArgs(args.show)

  val Member: AttributeKey[String] = AttributeKey.string("db.redis.member")

  val GeoUnit: AttributeKey[String] = AttributeKey.string("db.redis.geoUnit")
  def geoUnit(unit: io.lettuce.core.GeoArgs.Unit): Attribute[String] = GeoUnit(unit.name())

  val GeoValues: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.geoValues")

  val GeoArgs: AttributeKey[String] = AttributeKey.string("db.redis.geoArgs")
  def geoArgs(args: core.GeoArgs): Attribute[String] = GeoArgs(args.show)

  val OutputKey: AttributeKey[String] = AttributeKey.string("db.redis.outputKey")
  val InputKeys: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.inputKeys")

  val Cursor: AttributeKey[Long] = AttributeKey.long("db.redis.cursor")
  val CursorAsKeyScanCursor: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.cursor")
  val KeyScanCursor: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.keyScanCursor")
  val KeyScanArgs: AttributeKey[String] = AttributeKey.string("db.redis.keyScanArgs")
  val ScanArgs: AttributeKey[String] = AttributeKey.string("db.redis.scanArgs")
  val Previous: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.previous")

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

  /** Unix epoc timestamp in milliseconds. */
  val At: AttributeKey[Long] = AttributeKey.long("db.redis.at")
  def at(at: Instant): Attribute[Long] = At(at.toEpochMilli)
}
object CoreAttributes extends CoreAttributes
