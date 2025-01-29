package dev.profunktor.redis4cats.otel4s

import cats.Functor
import cats.data.NonEmptyList
import cats.syntax.functor.*
import dev.profunktor.redis4cats.RedisCommands
import dev.profunktor.redis4cats.algebra.BitCommandOperation
import dev.profunktor.redis4cats.data
import dev.profunktor.redis4cats.effects
import dev.profunktor.redis4cats.tx.TxStore
import io.lettuce.core.GeoArgs
import io.lettuce.core.RedisFuture
import io.lettuce.core.ZAddArgs
import io.lettuce.core.ZAggregateArgs
import io.lettuce.core.ZStoreArgs
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.AttributeKey
import org.typelevel.otel4s.trace.SpanBuilder
import org.typelevel.otel4s.trace.TracerProvider

import java.time.Instant
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import org.typelevel.otel4s.trace.Tracer

object TracedRedisCommands {
  object Attributes {
    val Key: AttributeKey[String] = AttributeKey.string("redis.key")
    val Keys: AttributeKey[Seq[String]] = AttributeKey.stringSeq("redis.keys")
    val KeyValuePairs: AttributeKey[Seq[String]] = AttributeKey.stringSeq("redis.keyValuePairs")
    val Value: AttributeKey[String] = AttributeKey.string("redis.value")
    val Values: AttributeKey[Seq[String]] = AttributeKey.stringSeq("redis.values")
    val GetExArg: AttributeKey[String] = AttributeKey.string("redis.getExArg")
    val Start: AttributeKey[Long] = AttributeKey.long("redis.start")
    val Stop: AttributeKey[Long] = AttributeKey.long("redis.stop")
    val End: AttributeKey[Long] = AttributeKey.long("redis.end")
    val SetArgs: AttributeKey[String] = AttributeKey.string("redis.setArgs")

    /** Expiration time in milliseconds. */
    val ExpiresIn: AttributeKey[Long] = AttributeKey.long("redis.expiresIn")
    def expiresIn(fd: FiniteDuration): Attribute[Long] = ExpiresIn(fd.toMillis)

    val Offset: AttributeKey[Long] = AttributeKey.long("redis.offset")
    val Amount: AttributeKey[Long] = AttributeKey.long("redis.amount")
    val AmountDouble: AttributeKey[Double] = AttributeKey.double("redis.amount")
    val Field: AttributeKey[String] = AttributeKey.string("redis.field")
    val Fields: AttributeKey[Seq[String]] = AttributeKey.stringSeq("redis.fields")
    val FieldValues: AttributeKey[Seq[String]] = AttributeKey.stringSeq("redis.fieldValues")
    val Count: AttributeKey[Long] = AttributeKey.long("redis.count")
    val Destination: AttributeKey[String] = AttributeKey.string("redis.destination")

    val Range: AttributeKey[String] = AttributeKey.string("redis.range")
    def range[A](range: effects.ZRange[A]): Attribute[String] = Range(range.toString)

    val RangeLimitOffset: AttributeKey[Long] = AttributeKey.long("redis.rangeLimit.offset")
    val RangeLimitCount: AttributeKey[Long] = AttributeKey.long("redis.rangeLimit.count")
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

    val Timeout: AttributeKey[Long] = AttributeKey.long("redis.timeout")
    def timeout(duration: Duration): Attribute[Long] = Timeout(durationAsLong(duration))

    val AggregateArgs: AttributeKey[String] = AttributeKey.string("redis.aggregateArgs")
    def aggregateArgs(args: ZAggregateArgs): Attribute[String] = AggregateArgs(args.toString)
    def aggregateArgs(args: Option[ZAggregateArgs]): Option[Attribute[String]] = args match {
      case None       => None
      case Some(args) => Some(aggregateArgs(args))
    }

    val ScoreWithValueScore: AttributeKey[Double] = AttributeKey.double("redis.scoreWithValue.score")
    val ScoreWithValueValue: AttributeKey[String] = AttributeKey.string("redis.scoreWithValue.value")
  }

  def apply[F[_]: Functor, K, V](
      cmd: RedisCommands[F, K, V],
      configureSpanBuilder: SpanBuilder[F] => SpanBuilder[F],
      recordKey: Option[K => String],
      recordValue: Option[V => String]
  )(implicit tracerProvider: TracerProvider[F]): F[RedisCommands[F, K, V]] = {
    tracerProvider.tracer("dev.profunktor.redis4cats.otel4s").withVersion("TODO").get.map { tracer =>
      new TracedRedisCommands(recordKey, recordValue, tracer, configureSpanBuilder, cmd)
    }
  }
}
class TracedRedisCommands[F[_], K, V](
    recordKey: Option[K => String],
    recordValue: Option[V => String],
    tracer: Tracer[F],
    configureSpanBuilder: SpanBuilder[F] => SpanBuilder[F],
    cmd: RedisCommands[F, K, V]
) extends RedisCommands[F, K, V] {
  import TracedRedisCommands.Attributes

  // We use raw pattern matching in these helpers for performance.

  def map[A](range: effects.ZRange[A], mapper: Option[A => String]): Option[effects.ZRange[String]] = mapper match {
    case None         => None
    case Some(mapper) => Some(effects.ZRange(mapper(range.start), mapper(range.end)))
  }

  def mapAsAttribute[A](
      range: effects.ZRange[A],
      mapper: Option[A => String],
      attr: AttributeKey[String] = Attributes.Range
  ): Option[Attribute[String]] =
    map(range, mapper) match {
      case None        => None
      case Some(range) => Some(attr(range.toString()))
    }

  def map[A](
      scoreWithValue: effects.ScoreWithValue[A],
      mapper: Option[A => String]
  ): Option[effects.ScoreWithValue[String]] = mapper match {
    case None         => None
    case Some(mapper) => Some(effects.ScoreWithValue(scoreWithValue.score, mapper(scoreWithValue.value)))
  }

  def mapAsAttribute[A](
      scoreWithValue: effects.ScoreWithValue[A],
      mapper: Option[A => String],
      attr: AttributeKey[String] = Attributes.ScoreWithValue
  ): Option[Attribute[String]] =
    map(scoreWithValue, mapper) match {
      case None        => None
      case Some(range) => Some(attr(range.toString()))
    }

  def span[A](name: String, attributes: collection.immutable.Iterable[Attribute[_]] = Nil)(fa: F[A]): F[A] =
    configureSpanBuilder(tracer.spanBuilder(name).addAttributes(attributes)).build.surround(fa)

  def keyAsAttribute(key: K, attr: AttributeKey[String] = Attributes.Key): Option[Attribute[String]] =
    recordKey match {
      case None      => None
      case Some(kFn) => Some(attr(kFn(key)))
    }

  def keysAsAttribute(
      key: K,
      others: Iterable[K],
      attr: AttributeKey[Seq[String]] = Attributes.Keys
  ): Option[Attribute[Seq[String]]] =
    recordKey match {
      case None      => None
      case Some(kFn) => Some(attr((Iterator(key) ++ others.iterator).map(kFn).toSeq))
    }

  def keys2AsAttribute(
      keys: Iterable[K],
      attr: AttributeKey[Seq[String]] = Attributes.Keys
  ): Option[Attribute[Seq[String]]] =
    recordKey match {
      case None      => None
      case Some(kFn) => Some(attr(keys.iterator.map(kFn).toSeq))
    }

  def valueAsAttribute(value: V): Option[Attribute[String]] = recordValue.map(v => Attributes.Value(v(value)))

  def valuesAsAttribute(
      value: V,
      others: Iterable[V],
      attr: AttributeKey[Seq[String]] = Attributes.Values
  ): Option[Attribute[Seq[String]]] =
    recordValue match {
      case None      => None
      case Some(vFn) => Some(attr((Iterator(value) ++ others.iterator).map(vFn).toSeq))
    }

  def values2AsAttribute(
      values: Iterable[V],
      attr: AttributeKey[Seq[String]] = Attributes.Values
  ): Option[Attribute[Seq[String]]] =
    recordValue match {
      case None      => None
      case Some(vFn) => Some(attr(values.iterator.map(vFn).toSeq))
    }

  def kvsAsAttribute(
      kvs: Map[K, V],
      attr: AttributeKey[Seq[String]] = Attributes.KeyValuePairs
  ): Option[Attribute[Seq[String]]] = recordKey match {
    case None => None
    case Some(kFn) =>
      recordValue match {
        case None =>
          Some(attr(kvs.keysIterator.map(k => s"${kFn(k)}=<unserialized>").toSeq))
        case Some(vFn) =>
          Some(attr(kvs.iterator.map { case (k, v) => s"${kFn(k)}=${vFn(v)}" }.toSeq))
      }
  }

  def kvAsAttributes(key: K, value: V): List[Attribute[String]] = {
    keyAsAttribute(key) match {
      case None               => valueAsAttribute(value).toList
      case Some(keyAttribute) => keyAttribute :: valueAsAttribute(value).toList
    }
  }

  override def get(key: K): F[Option[V]] =
    span("get", keyAsAttribute(key).toList)(cmd.get(key))

  override def getEx(key: K, getExArg: effects.GetExArg): F[Option[V]] =
    span("getEx", Attributes.GetExArg(getExArg.toString) :: keyAsAttribute(key).toList)(cmd.getEx(key, getExArg))

  override def getRange(key: K, start: Long, end: Long): F[Option[V]] =
    span("getRange", Attributes.Start(start) :: Attributes.End(end) :: keyAsAttribute(key).toList)(
      cmd.getRange(key, start, end)
    )

  override def strLen(key: K): F[Long] =
    span("strLen", keyAsAttribute(key).toList)(cmd.strLen(key))

  override def append(key: K, value: V): F[Unit] =
    span("append", kvAsAttributes(key, value))(cmd.append(key, value))

  override def getSet(key: K, value: V): F[Option[V]] =
    span("getSet", kvAsAttributes(key, value))(cmd.getSet(key, value))

  override def set(key: K, value: V): F[Unit] =
    span("set", kvAsAttributes(key, value))(cmd.set(key, value))

  override def set(key: K, value: V, setArgs: effects.SetArgs): F[Boolean] =
    span(
      "set",
      Attributes.SetArgs(setArgs.toString) :: kvAsAttributes(key, value)
    )(cmd.set(key, value, setArgs))

  override def setNx(key: K, value: V): F[Boolean] =
    span("setNx", kvAsAttributes(key, value))(cmd.setNx(key, value))

  override def setEx(key: K, value: V, expiresIn: FiniteDuration): F[Unit] =
    span("setEx", Attributes.expiresIn(expiresIn) :: kvAsAttributes(key, value))(cmd.setEx(key, value, expiresIn))

  override def setRange(key: K, value: V, offset: Long): F[Unit] =
    span("setRange", Attributes.Offset(offset) :: kvAsAttributes(key, value))(cmd.setRange(key, value, offset))

  override def mGet(keys: Set[K]): F[Map[K, V]] =
    span("mGet", keys2AsAttribute(keys).toList)(cmd.mGet(keys))

  override def mSet(keyValues: Map[K, V]): F[Unit] =
    span("mSet", kvsAsAttribute(keyValues).toList)(cmd.mSet(keyValues))

  override def mSetNx(keyValues: Map[K, V]): F[Boolean] =
    span("mSetNx", kvsAsAttribute(keyValues).toList)(cmd.mSetNx(keyValues))

  override def decr(key: K): F[Long] =
    span("decr", keyAsAttribute(key).toList)(cmd.decr(key))

  override def decrBy(key: K, amount: Long): F[Long] =
    span("decrBy", Attributes.Amount(amount) :: keyAsAttribute(key).toList)(cmd.decrBy(key, amount))

  override def incr(key: K): F[Long] =
    span("incr", keyAsAttribute(key).toList)(cmd.incr(key))

  override def incrBy(key: K, amount: Long): F[Long] =
    span("incrBy", Attributes.Amount(amount) :: keyAsAttribute(key).toList)(cmd.incrBy(key, amount))

  override def incrByFloat(key: K, amount: Double): F[Double] =
    span("incrByFloat", Attributes.AmountDouble(amount) :: keyAsAttribute(key).toList)(cmd.incrByFloat(key, amount))

  override def unsafe[A](f: RedisClusterAsyncCommands[K, V] => RedisFuture[A]): F[A] =
    span("unsafe")(cmd.unsafe(f))

  override def unsafeSync[A](f: RedisClusterAsyncCommands[K, V] => A): F[A] =
    span("unsafeSync")(cmd.unsafeSync(f))

  override def hGet(key: K, field: K): F[Option[V]] =
    span("hGet", keyAsAttribute(field, Attributes.Field).toList ::: keyAsAttribute(key).toList)(cmd.hGet(key, field))

  override def hGetAll(key: K): F[Map[K, V]] =
    span("hGetAll", keyAsAttribute(key).toList)(cmd.hGetAll(key))

  override def hmGet(key: K, field: K, fields: K*): F[Map[K, V]] =
    span("hmGet", keysAsAttribute(field, fields, Attributes.Fields).toList ::: keyAsAttribute(key).toList)(
      cmd.hmGet(key, field, fields*)
    )

  override def hKeys(key: K): F[List[K]] =
    span("hKeys", keyAsAttribute(key).toList)(cmd.hKeys(key))

  override def hVals(key: K): F[List[V]] =
    span("hVals", keyAsAttribute(key).toList)(cmd.hVals(key))

  override def hStrLen(key: K, field: K): F[Long] =
    span("hStrLen", keyAsAttribute(field, Attributes.Field).toList ::: keyAsAttribute(key).toList)(
      cmd.hStrLen(key, field)
    )

  override def hLen(key: K): F[Long] =
    span("hLen", keyAsAttribute(key).toList)(cmd.hLen(key))

  override def hSet(key: K, field: K, value: V): F[Boolean] =
    span("hSet", keyAsAttribute(field, Attributes.Field).toList ::: kvAsAttributes(key, value))(
      cmd.hSet(key, field, value)
    )

  override def hSet(key: K, fieldValues: Map[K, V]): F[Long] =
    span("hSet", kvsAsAttribute(fieldValues, Attributes.FieldValues).toList)(cmd.hSet(key, fieldValues))

  override def hSetNx(key: K, field: K, value: V): F[Boolean] =
    span("hSetNx", keyAsAttribute(field, Attributes.Field).toList ::: kvAsAttributes(key, value))(
      cmd.hSetNx(key, field, value)
    )

  override def hmSet(key: K, fieldValues: Map[K, V]): F[Unit] =
    span("hmSet", kvsAsAttribute(fieldValues, Attributes.FieldValues).toList)(cmd.hmSet(key, fieldValues))

  override def hIncrBy(key: K, field: K, amount: Long): F[Long] =
    span(
      "hIncrBy",
      keyAsAttribute(field, Attributes.Field).toList ::: Attributes.Amount(amount) :: keyAsAttribute(key).toList
    )(cmd.hIncrBy(key, field, amount))

  override def hIncrByFloat(key: K, field: K, amount: Double): F[Double] =
    span(
      "hIncrByFloat",
      keyAsAttribute(field, Attributes.Field).toList ::: Attributes.AmountDouble(amount) :: keyAsAttribute(key).toList
    )(cmd.hIncrByFloat(key, field, amount))

  override def hDel(key: K, field: K, fields: K*): F[Long] =
    span("hDel", keysAsAttribute(field, fields, Attributes.Fields).toList ::: keyAsAttribute(key).toList)(
      cmd.hDel(key, field, fields*)
    )

  override def hExists(key: K, field: K): F[Boolean] =
    span("hExists", keyAsAttribute(field, Attributes.Field).toList ::: keyAsAttribute(key).toList)(
      cmd.hExists(key, field)
    )

  override def sCard(key: K): F[Long] =
    span("sCard", keyAsAttribute(key).toList)(cmd.sCard(key))

  override def sDiff(keys: K*): F[Set[V]] =
    span("sDiff", keys2AsAttribute(keys).toList)(cmd.sDiff(keys*))

  override def sInter(keys: K*): F[Set[V]] =
    span("sInter", keys2AsAttribute(keys).toList)(cmd.sInter(keys*))

  override def sMembers(key: K): F[Set[V]] =
    span("sMembers", keyAsAttribute(key).toList)(cmd.sMembers(key))

  override def sRandMember(key: K): F[Option[V]] =
    span("sRandMember", keyAsAttribute(key).toList)(cmd.sRandMember(key))

  override def sRandMember(key: K, count: Long): F[List[V]] =
    span("sRandMember", Attributes.Count(count) :: keyAsAttribute(key).toList)(cmd.sRandMember(key, count))

  override def sUnion(keys: K*): F[Set[V]] =
    span("sUnion", keys2AsAttribute(keys).toList)(cmd.sUnion(keys*))

  override def sUnionStore(destination: K, keys: K*): F[Unit] =
    span("sUnionStore", keys2AsAttribute(keys).toList ::: keyAsAttribute(destination, Attributes.Destination).toList)(
      cmd.sUnionStore(destination, keys*)
    )

  override def sAdd(key: K, values: V*): F[Long] =
    span("sAdd", values2AsAttribute(values).toList ::: keyAsAttribute(key).toList)(cmd.sAdd(key, values*))

  override def sDiffStore(destination: K, keys: K*): F[Long] =
    span("sDiffStore", keys2AsAttribute(keys).toList ::: keyAsAttribute(destination, Attributes.Destination).toList)(
      cmd.sDiffStore(destination, keys*)
    )

  override def sInterStore(destination: K, keys: K*): F[Long] =
    span("sInterStore", keys2AsAttribute(keys).toList ::: keyAsAttribute(destination, Attributes.Destination).toList)(
      cmd.sInterStore(destination, keys*)
    )

  override def sMove(source: K, destination: K, value: V): F[Boolean] =
    span("sMove", keyAsAttribute(source).toList ::: keyAsAttribute(destination).toList)(
      cmd.sMove(source, destination, value)
    )

  override def sPop(key: K): F[Option[V]] =
    span("sPop", keyAsAttribute(key).toList)(cmd.sPop(key))

  override def sPop(key: K, count: Long): F[Set[V]] =
    span("sPop", Attributes.Count(count) :: keyAsAttribute(key).toList)(cmd.sPop(key, count))

  override def sRem(key: K, values: V*): F[Long] =
    span("sRem", values2AsAttribute(values).toList ::: keyAsAttribute(key).toList)(cmd.sRem(key, values*))

  override def sIsMember(key: K, value: V): F[Boolean] =
    span("sIsMember", kvAsAttributes(key, value).toList)(cmd.sIsMember(key, value))

  override def sMisMember(key: K, values: V*): F[List[Boolean]] =
    span("sMisMember", values2AsAttribute(values).toList ::: keyAsAttribute(key).toList)(cmd.sMisMember(key, values*))

  override def zCard(key: K): F[Long] =
    span("zCard", keyAsAttribute(key).toList)(cmd.zCard(key))

  override def zCount[T: Numeric](key: K, range: effects.ZRange[T]): F[Long] =
    span("zCount", Attributes.Range(range.toString) :: keyAsAttribute(key).toList)(cmd.zCount(key, range))

  override def zLexCount(key: K, range: effects.ZRange[V]): F[Long] =
    span(
      "zLexCount",
      mapAsAttribute(range, recordValue).toList ::: keyAsAttribute(key).toList
    )(cmd.zLexCount(key, range))

  override def zRange(key: K, start: Long, stop: Long): F[List[V]] =
    span("zRange", Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList)(
      cmd.zRange(key, start, stop)
    )

  override def zRangeByLex(key: K, range: effects.ZRange[V], limit: Option[effects.RangeLimit]): F[List[V]] =
    span(
      "zRangeByLex",
      Attributes.rangeLimit(limit) ::: mapAsAttribute(range, recordValue).toList ::: keyAsAttribute(key).toList
    )(cmd.zRangeByLex(key, range, limit))

  override def zRangeByScore[T: Numeric](
      key: K,
      range: effects.ZRange[T],
      limit: Option[effects.RangeLimit]
  ): F[List[V]] =
    span(
      "zRangeByScore",
      Attributes.rangeLimit(limit) ::: Attributes.range(range) :: keyAsAttribute(key).toList
    )(cmd.zRangeByScore(key, range, limit))

  override def zRangeByScoreWithScores[T: Numeric](
      key: K,
      range: effects.ZRange[T],
      limit: Option[effects.RangeLimit]
  ): F[List[effects.ScoreWithValue[V]]] =
    span(
      "zRangeByScoreWithScores",
      Attributes.rangeLimit(limit) ::: Attributes.range(range) :: keyAsAttribute(key).toList
    )(cmd.zRangeByScoreWithScores(key, range, limit))

  override def zRangeWithScores(key: K, start: Long, stop: Long): F[List[effects.ScoreWithValue[V]]] =
    span(
      "zRangeWithScores",
      Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList
    )(cmd.zRangeWithScores(key, start, stop))

  override def zRank(key: K, value: V): F[Option[Long]] =
    span("zRank", kvAsAttributes(key, value).toList)(cmd.zRank(key, value))

  override def zRevRange(key: K, start: Long, stop: Long): F[List[V]] =
    span(
      "zRevRange",
      Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList
    )(cmd.zRevRange(key, start, stop))

  override def zRevRangeByLex(key: K, range: effects.ZRange[V], limit: Option[effects.RangeLimit]): F[List[V]] =
    span(
      "zRevRangeByLex",
      Attributes.rangeLimit(limit) ::: mapAsAttribute(range, recordValue).toList ::: keyAsAttribute(key).toList
    )(cmd.zRevRangeByLex(key, range, limit))

  override def zRevRangeByScore[T: Numeric](
      key: K,
      range: effects.ZRange[T],
      limit: Option[effects.RangeLimit]
  ): F[List[V]] =
    span(
      "zRevRangeByScore",
      Attributes.rangeLimit(limit) ::: Attributes.range(range) :: keyAsAttribute(key).toList
    )(cmd.zRevRangeByScore(key, range, limit))

  override def zRevRangeByScoreWithScores[T: Numeric](
      key: K,
      range: effects.ZRange[T],
      limit: Option[effects.RangeLimit]
  ): F[List[effects.ScoreWithValue[V]]] =
    span(
      "zRevRangeByScoreWithScores",
      Attributes.rangeLimit(limit) ::: Attributes.range(range) :: keyAsAttribute(key).toList
    )(cmd.zRevRangeByScoreWithScores(key, range, limit))

  override def zRevRangeWithScores(key: K, start: Long, stop: Long): F[List[effects.ScoreWithValue[V]]] =
    span(
      "zRevRangeWithScores",
      Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList
    )(cmd.zRevRangeWithScores(key, start, stop))

  override def zRevRank(key: K, value: V): F[Option[Long]] =
    span("zRevRank", kvAsAttributes(key, value).toList)(cmd.zRevRank(key, value))

  override def zScore(key: K, value: V): F[Option[Double]] =
    span("zScore", kvAsAttributes(key, value).toList)(cmd.zScore(key, value))

  override def zPopMin(key: K, count: Long): F[List[effects.ScoreWithValue[V]]] =
    span("zPopMin", Attributes.Count(count) :: keyAsAttribute(key).toList)(cmd.zPopMin(key, count))

  override def zPopMax(key: K, count: Long): F[List[effects.ScoreWithValue[V]]] =
    span("zPopMax", Attributes.Count(count) :: keyAsAttribute(key).toList)(cmd.zPopMax(key, count))

  override def bzPopMax(timeout: Duration, keys: NonEmptyList[K]): F[Option[(K, effects.ScoreWithValue[V])]] =
    span("bzPopMax", Attributes.timeout(timeout) :: keys2AsAttribute(keys.toList).toList)(cmd.bzPopMax(timeout, keys))

  override def bzPopMin(timeout: Duration, keys: NonEmptyList[K]): F[Option[(K, effects.ScoreWithValue[V])]] =
    span("bzPopMin", Attributes.timeout(timeout) :: keys2AsAttribute(keys.toList).toList)(cmd.bzPopMin(timeout, keys))

  override def zUnion(args: Option[ZAggregateArgs], keys: K*): F[List[V]] =
    span("zUnion", Attributes.aggregateArgs(args).toList ::: keys2AsAttribute(keys).toList)(cmd.zUnion(args, keys*))

  override def zUnionWithScores(args: Option[ZAggregateArgs], keys: K*): F[List[effects.ScoreWithValue[V]]] =
    span("zUnionWithScores", Attributes.aggregateArgs(args).toList ::: keys2AsAttribute(keys).toList)(
      cmd.zUnionWithScores(args, keys*)
    )

  override def zInter(args: Option[ZAggregateArgs], keys: K*): F[List[V]] =
    span("zInter", Attributes.aggregateArgs(args).toList ::: keys2AsAttribute(keys).toList)(cmd.zInter(args, keys*))

  override def zInterWithScores(args: Option[ZAggregateArgs], keys: K*): F[List[effects.ScoreWithValue[V]]] =
    span("zInterWithScores", Attributes.aggregateArgs(args).toList ::: keys2AsAttribute(keys).toList)(
      cmd.zInterWithScores(args, keys*)
    )

  override def zDiff(keys: K*): F[List[V]] =
    span("zDiff", keys2AsAttribute(keys).toList)(cmd.zDiff(keys*))

  override def zDiffWithScores(keys: K*): F[List[effects.ScoreWithValue[V]]] =
    span("zDiffWithScores", keys2AsAttribute(keys).toList)(cmd.zDiffWithScores(keys*))

  override def zAdd(key: K, args: Option[ZAddArgs], values: effects.ScoreWithValue[V]*): F[Long] =
    span(
      "zAdd",
      Attributes.aggregateArgs(args).toList ::: mapAsAttribute(values, recordValue).toList ::: keyAsAttribute(
        key
      ).toList
    )(cmd.zAdd(key, args, values*))

  override def zAddIncr(key: K, args: Option[ZAddArgs], value: effects.ScoreWithValue[V]): F[Double] = ???

  override def zIncrBy(key: K, member: V, amount: Double): F[Double] = ???

  override def zInterStore(destination: K, args: Option[ZStoreArgs], keys: K*): F[Long] = ???

  override def zRem(key: K, values: V*): F[Long] = ???

  override def zRemRangeByLex(key: K, range: effects.ZRange[V]): F[Long] = ???

  override def zRemRangeByRank(key: K, start: Long, stop: Long): F[Long] = ???

  override def zRemRangeByScore[T: Numeric](key: K, range: effects.ZRange[T]): F[Long] = ???

  override def zUnionStore(destination: K, args: Option[ZStoreArgs], keys: K*): F[Long] = ???

  override def blPop(timeout: Duration, keys: NonEmptyList[K]): F[Option[(K, V)]] = ???

  override def brPop(timeout: Duration, keys: NonEmptyList[K]): F[Option[(K, V)]] = ???

  override def brPopLPush(timeout: Duration, source: K, destination: K): F[Option[V]] = ???

  override def lIndex(key: K, index: Long): F[Option[V]] = ???

  override def lLen(key: K): F[Long] = ???

  override def lRange(key: K, start: Long, stop: Long): F[List[V]] = ???

  override def lInsertAfter(key: K, pivot: V, value: V): F[Long] = ???

  override def lInsertBefore(key: K, pivot: V, value: V): F[Long] = ???

  override def lRem(key: K, count: Long, value: V): F[Long] = ???

  override def lSet(key: K, index: Long, value: V): F[Unit] = ???

  override def lTrim(key: K, start: Long, stop: Long): F[Unit] = ???

  override def lPop(key: K): F[Option[V]] = ???

  override def lPush(key: K, values: V*): F[Long] = ???

  override def lPushX(key: K, values: V*): F[Long] = ???

  override def rPop(key: K): F[Option[V]] = ???

  override def rPopLPush(source: K, destination: K): F[Option[V]] = ???

  override def rPush(key: K, values: V*): F[Long] = ???

  override def rPushX(key: K, values: V*): F[Long] = ???

  override def geoDist(key: K, from: V, to: V, unit: GeoArgs.Unit): F[Double] = ???

  override def geoHash(key: K, values: V*): F[List[Option[String]]] = ???

  override def geoPos(key: K, values: V*): F[List[effects.GeoCoordinate]] = ???

  override def geoRadius(key: K, geoRadius: effects.GeoRadius, unit: GeoArgs.Unit): F[Set[V]] = ???

  override def geoRadius(
      key: K,
      geoRadius: effects.GeoRadius,
      unit: GeoArgs.Unit,
      args: GeoArgs
  ): F[List[effects.GeoRadiusResult[V]]] = ???

  override def geoRadiusByMember(key: K, value: V, dist: effects.Distance, unit: GeoArgs.Unit): F[Set[V]] = ???

  override def geoRadiusByMember(
      key: K,
      value: V,
      dist: effects.Distance,
      unit: GeoArgs.Unit,
      args: GeoArgs
  ): F[List[effects.GeoRadiusResult[V]]] = ???

  override def geoAdd(key: K, geoValues: effects.GeoLocation[V]*): F[Unit] = ???

  override def geoRadius(
      key: K,
      geoRadius: effects.GeoRadius,
      unit: GeoArgs.Unit,
      storage: effects.GeoRadiusKeyStorage[K]
  ): F[Unit] = ???

  override def geoRadius(
      key: K,
      geoRadius: effects.GeoRadius,
      unit: GeoArgs.Unit,
      storage: effects.GeoRadiusDistStorage[K]
  ): F[Unit] = ???

  override def geoRadiusByMember(
      key: K,
      value: V,
      dist: effects.Distance,
      unit: GeoArgs.Unit,
      storage: effects.GeoRadiusKeyStorage[K]
  ): F[Unit] = ???

  override def geoRadiusByMember(
      key: K,
      value: V,
      dist: effects.Distance,
      unit: GeoArgs.Unit,
      storage: effects.GeoRadiusDistStorage[K]
  ): F[Unit] = ???

  override def ping: F[String] = ???

  override def select(index: Int): F[Unit] = ???

  override def auth(password: CharSequence): F[Boolean] = ???

  override def auth(username: String, password: CharSequence): F[Boolean] = ???

  override def setClientName(name: K): F[Boolean] = ???

  override def getClientName(): F[Option[K]] = ???

  override def getClientId(): F[Long] = ???

  override def getClientInfo: F[Map[String, String]] = ???

  override def setLibName(name: String): F[Boolean] = ???

  override def setLibVersion(version: String): F[Boolean] = ???

  override def keys(key: K): F[List[K]] = ???

  override def flushAll: F[Unit] = ???

  override def flushAll(mode: effects.FlushMode): F[Unit] = ???

  override def flushDb: F[Unit] = ???

  override def flushDb(mode: effects.FlushMode): F[Unit] = ???

  override def info: F[Map[String, String]] = ???

  override def info(section: String): F[Map[String, String]] = ???

  override def dbsize: F[Long] = ???

  override def lastSave: F[Instant] = ???

  override def slowLogLen: F[Long] = ???

  override def multi: F[Unit] = ???

  override def exec: F[Unit] = ???

  override def discard: F[Unit] = ???

  override def watch(keys: K*): F[Unit] = ???

  override def unwatch: F[Unit] = ???

  override def transact[A](fs: TxStore[F, String, A] => List[F[Unit]]): F[Map[String, A]] = ???

  override def transact_(fs: List[F[Unit]]): F[Unit] = ???

  override def pipeline[A](fs: TxStore[F, String, A] => List[F[Unit]]): F[Map[String, A]] = ???

  override def pipeline_(fs: List[F[Unit]]): F[Unit] = ???

  override def enableAutoFlush: F[Unit] = ???

  override def disableAutoFlush: F[Unit] = ???

  override def flushCommands: F[Unit] = ???

  override def eval(script: String, output: effects.ScriptOutputType[V]): F[output.R] = ???

  override def eval(script: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] = ???

  override def eval(script: String, output: effects.ScriptOutputType[V], keys: List[K], values: List[V]): F[output.R] =
    ???

  override def evalReadOnly(script: String, output: effects.ScriptOutputType[V]): F[output.R] = ???

  override def evalReadOnly(script: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] = ???

  override def evalReadOnly(
      script: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] = ???

  override def evalSha(digest: String, output: effects.ScriptOutputType[V]): F[output.R] = ???

  override def evalSha(digest: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] = ???

  override def evalSha(
      digest: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] = ???

  override def evalShaReadOnly(digest: String, output: effects.ScriptOutputType[V]): F[output.R] = ???

  override def evalShaReadOnly(digest: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] =
    ???

  override def evalShaReadOnly(
      digest: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] = ???

  override def scriptLoad(script: String): F[String] = ???

  override def scriptLoad(script: Array[Byte]): F[String] = ???

  override def scriptExists(digests: String*): F[List[Boolean]] = ???

  override def scriptFlush: F[Unit] = ???

  override def digest(script: String): F[String] = ???

  override def fcall(function: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] = ???

  override def fcall(
      function: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] = ???

  override def fcallReadOnly(function: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] =
    ???

  override def fcallReadOnly(
      function: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] = ???

  override def functionLoad(functionCode: String): F[String] = ???

  override def functionLoad(functionCode: String, replace: Boolean): F[String] = ???

  override def functionDump(): F[Array[Byte]] = ???

  override def functionRestore(dump: Array[Byte]): F[String] = ???

  override def functionRestore(dump: Array[Byte], mode: effects.FunctionRestoreMode): F[String] = ???

  override def functionFlush(flushMode: effects.FlushMode): F[String] = ???

  override def functionKill(): F[String] = ???

  override def functionList(): F[List[Map[String, Any]]] = ???

  override def functionList(libraryName: String): F[List[Map[String, Any]]] = ???

  override def copy(source: K, destination: K): F[Boolean] = ???

  override def copy(source: K, destination: K, copyArgs: effects.CopyArgs): F[Boolean] = ???

  override def del(key: K*): F[Long] = ???

  override def dump(key: K): F[Option[Array[Byte]]] = ???

  override def exists(key: K*): F[Boolean] = ???

  override def expire(key: K, expiresIn: FiniteDuration): F[Boolean] = ???

  override def expire(key: K, expiresIn: FiniteDuration, expireExistenceArg: effects.ExpireExistenceArg): F[Boolean] =
    ???

  override def expireAt(key: K, at: Instant): F[Boolean] = ???

  override def expireAt(key: K, at: Instant, expireExistenceArg: effects.ExpireExistenceArg): F[Boolean] = ???

  override def objectIdletime(key: K): F[Option[FiniteDuration]] = ???

  override def persist(key: K): F[Boolean] = ???

  override def pttl(key: K): F[Option[FiniteDuration]] = ???

  override def randomKey: F[Option[K]] = ???

  override def restore(key: K, value: Array[Byte]): F[Unit] = ???

  override def restore(key: K, value: Array[Byte], restoreArgs: effects.RestoreArgs): F[Unit] = ???

  override def scan: F[data.KeyScanCursor[K]] = ???

  override def scan(cursor: Long): F[data.KeyScanCursor[K]] = ???

  override def scan(previous: data.KeyScanCursor[K]): F[data.KeyScanCursor[K]] = ???

  override def scan(scanArgs: effects.ScanArgs): F[data.KeyScanCursor[K]] = ???

  override def scan(keyScanArgs: effects.KeyScanArgs): F[data.KeyScanCursor[K]] = ???

  override def scan(cursor: Long, scanArgs: effects.ScanArgs): F[data.KeyScanCursor[K]] = ???

  override def scan(previous: data.KeyScanCursor[K], scanArgs: effects.ScanArgs): F[data.KeyScanCursor[K]] = ???

  override def scan(cursor: data.KeyScanCursor[K], keyScanArgs: effects.KeyScanArgs): F[data.KeyScanCursor[K]] =
    ???

  override def typeOf(key: K): F[Option[effects.RedisType]] = ???

  override def ttl(key: K): F[Option[FiniteDuration]] = ???

  override def unlink(key: K*): F[Long] = ???

  override def pfAdd(key: K, values: V*): F[Long] = ???

  override def pfCount(key: K): F[Long] = ???

  override def pfMerge(outputKey: K, inputKeys: K*): F[Unit] = ???

  override def bitCount(key: K): F[Long] = ???

  override def bitCount(key: K, start: Long, end: Long): F[Long] = ???

  override def bitField(key: K, operations: BitCommandOperation*): F[List[Long]] = ???

  override def bitOpAnd(destination: K, sources: K*): F[Unit] = ???

  override def bitOpNot(destination: K, source: K): F[Unit] = ???

  override def bitOpOr(destination: K, sources: K*): F[Unit] = ???

  override def bitOpXor(destination: K, sources: K*): F[Unit] = ???

  override def bitPos(key: K, state: Boolean): F[Long] = ???

  override def bitPos(key: K, state: Boolean, start: Long): F[Long] = ???

  override def bitPos(key: K, state: Boolean, start: Long, end: Long): F[Long] = ???

  override def getBit(key: K, offset: Long): F[Option[Long]] = ???

  override def setBit(key: K, offset: Long, value: Int): F[Long] = ???
}
