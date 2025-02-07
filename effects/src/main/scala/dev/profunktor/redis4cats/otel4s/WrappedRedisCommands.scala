package dev.profunktor.redis4cats.otel4s

import cats.data.NonEmptyList
import cats.syntax.functor.*
import cats.syntax.show.*
import dev.profunktor.redis4cats.algebra.BitCommandOperation
import dev.profunktor.redis4cats.tx.TxStore
import dev.profunktor.redis4cats.{RedisCommands, data, effects}
import io.lettuce.core.*
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands

import java.time.Instant
import scala.annotation.nowarn
import scala.concurrent.duration.{Duration, FiniteDuration}

/** Wraps every command in [[RedisCommands]]. This is used for tracing, but can be used for something else as well. */
// Do not warn about using deprecated APIs as we still have to wrap them.
@nowarn("cat=deprecation")
trait WrappedRedisCommands[F[_], K, V] extends RedisCommands[F, K, V] {
  import EffectsImplicits.*
  private def Attributes = EffectsAttributes

  /** The underlying instance. */
  def cmd: RedisCommands[F, K, V]

  /** The wrapper to use. */
  def wrapper: CommandWrapper[F]

  val helpers: WrappingHelpers[K, V]

  import helpers.*

  override def get(key: K): F[Option[V]] =
    wrapper.wrap("get", keyAsAttribute(key).toList)(cmd.get(key))

  override def getEx(key: K, getExArg: effects.GetExArg): F[Option[V]] =
    wrapper.wrap("getEx", Attributes.GetExArg(getExArg.toString) :: keyAsAttribute(key).toList)(
      cmd.getEx(key, getExArg)
    )

  override def getRange(key: K, start: Long, end: Long): F[Option[V]] =
    wrapper.wrap("getRange", Attributes.Start(start) :: Attributes.End(end) :: keyAsAttribute(key).toList)(
      cmd.getRange(key, start, end)
    )

  override def strLen(key: K): F[Long] =
    wrapper.wrap("strLen", keyAsAttribute(key).toList)(cmd.strLen(key))

  override def append(key: K, value: V): F[Unit] =
    wrapper.wrap("append", kvAsAttributes(key, value))(cmd.append(key, value))

  override def getSet(key: K, value: V): F[Option[V]] =
    wrapper.wrap("getSet", kvAsAttributes(key, value))(cmd.getSet(key, value))

  override def set(key: K, value: V): F[Unit] =
    wrapper.wrap("set", kvAsAttributes(key, value))(cmd.set(key, value))

  override def set(key: K, value: V, setArgs: effects.SetArgs): F[Boolean] =
    wrapper.wrap(
      "set",
      Attributes.SetArgs(setArgs.toString) :: kvAsAttributes(key, value)
    )(cmd.set(key, value, setArgs))

  override def setNx(key: K, value: V): F[Boolean] =
    wrapper.wrap("setNx", kvAsAttributes(key, value))(cmd.setNx(key, value))

  override def setEx(key: K, value: V, expiresIn: FiniteDuration): F[Unit] =
    wrapper.wrap("setEx", Attributes.expiresIn(expiresIn) :: kvAsAttributes(key, value))(
      cmd.setEx(key, value, expiresIn)
    )

  override def setRange(key: K, value: V, offset: Long): F[Unit] =
    wrapper.wrap("setRange", Attributes.Offset(offset) :: kvAsAttributes(key, value))(cmd.setRange(key, value, offset))

  override def mGet(keys: Set[K]): F[Map[K, V]] =
    wrapper.wrap("mGet", keys2AsAttribute(keys).toList)(cmd.mGet(keys))

  override def mSet(keyValues: Map[K, V]): F[Unit] =
    wrapper.wrap("mSet", kvsAsAttribute(keyValues).toList)(cmd.mSet(keyValues))

  override def mSetNx(keyValues: Map[K, V]): F[Boolean] =
    wrapper.wrap("mSetNx", kvsAsAttribute(keyValues).toList)(cmd.mSetNx(keyValues))

  override def decr(key: K): F[Long] =
    wrapper.wrap("decr", keyAsAttribute(key).toList)(cmd.decr(key))

  override def decrBy(key: K, amount: Long): F[Long] =
    wrapper.wrap("decrBy", Attributes.Amount(amount) :: keyAsAttribute(key).toList)(cmd.decrBy(key, amount))

  override def incr(key: K): F[Long] =
    wrapper.wrap("incr", keyAsAttribute(key).toList)(cmd.incr(key))

  override def incrBy(key: K, amount: Long): F[Long] =
    wrapper.wrap("incrBy", Attributes.Amount(amount) :: keyAsAttribute(key).toList)(cmd.incrBy(key, amount))

  override def incrByFloat(key: K, amount: Double): F[Double] =
    wrapper.wrap("incrByFloat", Attributes.AmountDouble(amount) :: keyAsAttribute(key).toList)(
      cmd.incrByFloat(key, amount)
    )

  override def unsafe[A](f: RedisClusterAsyncCommands[K, V] => RedisFuture[A]): F[A] =
    wrapper.wrap("unsafe")(cmd.unsafe(f))

  override def unsafeSync[A](f: RedisClusterAsyncCommands[K, V] => A): F[A] =
    wrapper.wrap("unsafeSync")(cmd.unsafeSync(f))

  override def hGet(key: K, field: K): F[Option[V]] =
    wrapper.wrap("hGet", keyAsAttribute(field, Attributes.Field).toList ::: keyAsAttribute(key).toList)(
      cmd.hGet(key, field)
    )

  override def hGetAll(key: K): F[Map[K, V]] =
    wrapper.wrap("hGetAll", keyAsAttribute(key).toList)(cmd.hGetAll(key))

  override def hmGet(key: K, field: K, fields: K*): F[Map[K, V]] =
    wrapper.wrap("hmGet", keysAsAttribute(field, fields, Attributes.Fields).toList ::: keyAsAttribute(key).toList)(
      cmd.hmGet(key, field, fields*)
    )

  override def hKeys(key: K): F[List[K]] =
    wrapper.wrap("hKeys", keyAsAttribute(key).toList)(cmd.hKeys(key))

  override def hVals(key: K): F[List[V]] =
    wrapper.wrap("hVals", keyAsAttribute(key).toList)(cmd.hVals(key))

  override def hStrLen(key: K, field: K): F[Long] =
    wrapper.wrap("hStrLen", keyAsAttribute(field, Attributes.Field).toList ::: keyAsAttribute(key).toList)(
      cmd.hStrLen(key, field)
    )

  override def hLen(key: K): F[Long] =
    wrapper.wrap("hLen", keyAsAttribute(key).toList)(cmd.hLen(key))

  override def hSet(key: K, field: K, value: V): F[Boolean] =
    wrapper.wrap("hSet", keyAsAttribute(field, Attributes.Field).toList ::: kvAsAttributes(key, value))(
      cmd.hSet(key, field, value)
    )

  override def hSet(key: K, fieldValues: Map[K, V]): F[Long] =
    wrapper.wrap("hSet", kvsAsAttribute(fieldValues, Attributes.FieldValues).toList)(cmd.hSet(key, fieldValues))

  override def hSetNx(key: K, field: K, value: V): F[Boolean] =
    wrapper.wrap("hSetNx", keyAsAttribute(field, Attributes.Field).toList ::: kvAsAttributes(key, value))(
      cmd.hSetNx(key, field, value)
    )

  override def hmSet(key: K, fieldValues: Map[K, V]): F[Unit] =
    wrapper.wrap("hmSet", kvsAsAttribute(fieldValues, Attributes.FieldValues).toList)(cmd.hmSet(key, fieldValues))

  override def hIncrBy(key: K, field: K, amount: Long): F[Long] =
    wrapper.wrap(
      "hIncrBy",
      keyAsAttribute(field, Attributes.Field).toList ::: Attributes.Amount(amount) :: keyAsAttribute(key).toList
    )(cmd.hIncrBy(key, field, amount))

  override def hIncrByFloat(key: K, field: K, amount: Double): F[Double] =
    wrapper.wrap(
      "hIncrByFloat",
      keyAsAttribute(field, Attributes.Field).toList ::: Attributes.AmountDouble(amount) :: keyAsAttribute(key).toList
    )(cmd.hIncrByFloat(key, field, amount))

  override def hDel(key: K, field: K, fields: K*): F[Long] =
    wrapper.wrap("hDel", keysAsAttribute(field, fields, Attributes.Fields).toList ::: keyAsAttribute(key).toList)(
      cmd.hDel(key, field, fields*)
    )

  override def hExists(key: K, field: K): F[Boolean] =
    wrapper.wrap("hExists", keyAsAttribute(field, Attributes.Field).toList ::: keyAsAttribute(key).toList)(
      cmd.hExists(key, field)
    )

  override def sCard(key: K): F[Long] =
    wrapper.wrap("sCard", keyAsAttribute(key).toList)(cmd.sCard(key))

  override def sDiff(keys: K*): F[Set[V]] =
    wrapper.wrap("sDiff", keys2AsAttribute(keys).toList)(cmd.sDiff(keys*))

  override def sInter(keys: K*): F[Set[V]] =
    wrapper.wrap("sInter", keys2AsAttribute(keys).toList)(cmd.sInter(keys*))

  override def sMembers(key: K): F[Set[V]] =
    wrapper.wrap("sMembers", keyAsAttribute(key).toList)(cmd.sMembers(key))

  override def sRandMember(key: K): F[Option[V]] =
    wrapper.wrap("sRandMember", keyAsAttribute(key).toList)(cmd.sRandMember(key))

  override def sRandMember(key: K, count: Long): F[List[V]] =
    wrapper.wrap("sRandMember", Attributes.Count(count) :: keyAsAttribute(key).toList)(cmd.sRandMember(key, count))

  override def sUnion(keys: K*): F[Set[V]] =
    wrapper.wrap("sUnion", keys2AsAttribute(keys).toList)(cmd.sUnion(keys*))

  override def sUnionStore(destination: K, keys: K*): F[Unit] =
    wrapper.wrap(
      "sUnionStore",
      keys2AsAttribute(keys).toList ::: keyAsAttribute(destination, Attributes.Destination).toList
    )(
      cmd.sUnionStore(destination, keys*)
    )

  override def sAdd(key: K, values: V*): F[Long] =
    wrapper.wrap("sAdd", values2AsAttribute(values).toList ::: keyAsAttribute(key).toList)(cmd.sAdd(key, values*))

  override def sDiffStore(destination: K, keys: K*): F[Long] =
    wrapper.wrap(
      "sDiffStore",
      keys2AsAttribute(keys).toList ::: keyAsAttribute(destination, Attributes.Destination).toList
    )(
      cmd.sDiffStore(destination, keys*)
    )

  override def sInterStore(destination: K, keys: K*): F[Long] =
    wrapper.wrap(
      "sInterStore",
      keys2AsAttribute(keys).toList ::: keyAsAttribute(destination, Attributes.Destination).toList
    )(
      cmd.sInterStore(destination, keys*)
    )

  override def sMove(source: K, destination: K, value: V): F[Boolean] =
    wrapper.wrap("sMove", keyAsAttribute(source).toList ::: keyAsAttribute(destination).toList)(
      cmd.sMove(source, destination, value)
    )

  override def sPop(key: K): F[Option[V]] =
    wrapper.wrap("sPop", keyAsAttribute(key).toList)(cmd.sPop(key))

  override def sPop(key: K, count: Long): F[Set[V]] =
    wrapper.wrap("sPop", Attributes.Count(count) :: keyAsAttribute(key).toList)(cmd.sPop(key, count))

  override def sRem(key: K, values: V*): F[Long] =
    wrapper.wrap("sRem", values2AsAttribute(values).toList ::: keyAsAttribute(key).toList)(cmd.sRem(key, values*))

  override def sIsMember(key: K, value: V): F[Boolean] =
    wrapper.wrap("sIsMember", kvAsAttributes(key, value).toList)(cmd.sIsMember(key, value))

  override def sMisMember(key: K, values: V*): F[List[Boolean]] =
    wrapper.wrap("sMisMember", values2AsAttribute(values).toList ::: keyAsAttribute(key).toList)(
      cmd.sMisMember(key, values*)
    )

  override def zCard(key: K): F[Long] =
    wrapper.wrap("zCard", keyAsAttribute(key).toList)(cmd.zCard(key))

  override def zCount[T: Numeric](key: K, range: effects.ZRange[T]): F[Long] =
    wrapper
      .wrap("zCount", Attributes.range(range.map(_.toString)) ::: keyAsAttribute(key).toList)(cmd.zCount(key, range))

  override def zLexCount(key: K, range: effects.ZRange[V]): F[Long] =
    wrapper.wrap(
      "zLexCount",
      recordValue.toList.flatMap(f => Attributes.range(range.map(f))) ::: keyAsAttribute(key).toList
    )(cmd.zLexCount(key, range))

  override def zRange(key: K, start: Long, stop: Long): F[List[V]] =
    wrapper.wrap("zRange", Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList)(
      cmd.zRange(key, start, stop)
    )

  override def zRangeByLex(key: K, range: effects.ZRange[V], limit: Option[effects.RangeLimit]): F[List[V]] =
    wrapper.wrap(
      "zRangeByLex",
      Attributes.rangeLimit(limit) ::: recordValue.toList.flatMap(f =>
        Attributes.range(range.map(f))
      ) ::: keyAsAttribute(key).toList
    )(cmd.zRangeByLex(key, range, limit))

  override def zRangeByScore[T: Numeric](
      key: K,
      range: effects.ZRange[T],
      limit: Option[effects.RangeLimit]
  ): F[List[V]] =
    wrapper.wrap(
      "zRangeByScore",
      Attributes.rangeLimit(limit) ::: Attributes.range(range.map(_.toString)) ::: keyAsAttribute(key).toList
    )(cmd.zRangeByScore(key, range, limit))

  override def zRangeByScoreWithScores[T: Numeric](
      key: K,
      range: effects.ZRange[T],
      limit: Option[effects.RangeLimit]
  ): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap(
      "zRangeByScoreWithScores",
      Attributes.rangeLimit(limit) ::: Attributes.range(range.map(_.toString)) ::: keyAsAttribute(key).toList
    )(cmd.zRangeByScoreWithScores(key, range, limit))

  override def zRangeWithScores(key: K, start: Long, stop: Long): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap(
      "zRangeWithScores",
      Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList
    )(cmd.zRangeWithScores(key, start, stop))

  override def zRank(key: K, value: V): F[Option[Long]] =
    wrapper.wrap("zRank", kvAsAttributes(key, value).toList)(cmd.zRank(key, value))

  override def zRevRange(key: K, start: Long, stop: Long): F[List[V]] =
    wrapper.wrap(
      "zRevRange",
      Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList
    )(cmd.zRevRange(key, start, stop))

  override def zRevRangeByLex(key: K, range: effects.ZRange[V], limit: Option[effects.RangeLimit]): F[List[V]] =
    wrapper.wrap(
      "zRevRangeByLex",
      Attributes.rangeLimit(limit) ::: recordValue.toList.flatMap(f =>
        Attributes.range(range.map(f))
      ) ::: keyAsAttribute(key).toList
    )(cmd.zRevRangeByLex(key, range, limit))

  override def zRevRangeByScore[T: Numeric](
      key: K,
      range: effects.ZRange[T],
      limit: Option[effects.RangeLimit]
  ): F[List[V]] =
    wrapper.wrap(
      "zRevRangeByScore",
      Attributes.rangeLimit(limit) ::: Attributes.range(range.map(_.toString)) ::: keyAsAttribute(key).toList
    )(cmd.zRevRangeByScore(key, range, limit))

  override def zRevRangeByScoreWithScores[T: Numeric](
      key: K,
      range: effects.ZRange[T],
      limit: Option[effects.RangeLimit]
  ): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap(
      "zRevRangeByScoreWithScores",
      Attributes.rangeLimit(limit) ::: Attributes.range(range.map(_.toString)) ::: keyAsAttribute(key).toList
    )(cmd.zRevRangeByScoreWithScores(key, range, limit))

  override def zRevRangeWithScores(key: K, start: Long, stop: Long): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap(
      "zRevRangeWithScores",
      Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList
    )(cmd.zRevRangeWithScores(key, start, stop))

  override def zRevRank(key: K, value: V): F[Option[Long]] =
    wrapper.wrap("zRevRank", kvAsAttributes(key, value).toList)(cmd.zRevRank(key, value))

  override def zScore(key: K, value: V): F[Option[Double]] =
    wrapper.wrap("zScore", kvAsAttributes(key, value).toList)(cmd.zScore(key, value))

  override def zPopMin(key: K, count: Long): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap("zPopMin", Attributes.Count(count) :: keyAsAttribute(key).toList)(cmd.zPopMin(key, count))

  override def zPopMax(key: K, count: Long): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap("zPopMax", Attributes.Count(count) :: keyAsAttribute(key).toList)(cmd.zPopMax(key, count))

  override def bzPopMax(timeout: Duration, keys: NonEmptyList[K]): F[Option[(K, effects.ScoreWithValue[V])]] =
    wrapper.wrap("bzPopMax", Attributes.timeout(timeout) :: keys2AsAttribute(keys.toList).toList)(
      cmd.bzPopMax(timeout, keys)
    )

  override def bzPopMin(timeout: Duration, keys: NonEmptyList[K]): F[Option[(K, effects.ScoreWithValue[V])]] =
    wrapper.wrap("bzPopMin", Attributes.timeout(timeout) :: keys2AsAttribute(keys.toList).toList)(
      cmd.bzPopMin(timeout, keys)
    )

  override def zUnion(args: Option[ZAggregateArgs], keys: K*): F[List[V]] =
    wrapper.wrap("zUnion", args.map(Attributes.aggregateArgs).toList ::: keys2AsAttribute(keys).toList)(
      cmd.zUnion(args, keys*)
    )

  override def zUnionWithScores(args: Option[ZAggregateArgs], keys: K*): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap("zUnionWithScores", args.map(Attributes.aggregateArgs).toList ::: keys2AsAttribute(keys).toList)(
      cmd.zUnionWithScores(args, keys*)
    )

  override def zInter(args: Option[ZAggregateArgs], keys: K*): F[List[V]] =
    wrapper.wrap("zInter", args.map(Attributes.aggregateArgs).toList ::: keys2AsAttribute(keys).toList)(
      cmd.zInter(args, keys*)
    )

  override def zInterWithScores(args: Option[ZAggregateArgs], keys: K*): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap("zInterWithScores", args.map(Attributes.aggregateArgs).toList ::: keys2AsAttribute(keys).toList)(
      cmd.zInterWithScores(args, keys*)
    )

  override def zDiff(keys: K*): F[List[V]] =
    wrapper.wrap("zDiff", keys2AsAttribute(keys).toList)(cmd.zDiff(keys*))

  override def zDiffWithScores(keys: K*): F[List[effects.ScoreWithValue[V]]] =
    wrapper.wrap("zDiffWithScores", keys2AsAttribute(keys).toList)(cmd.zDiffWithScores(keys*))

  override def zAdd(key: K, args: Option[ZAddArgs], values: effects.ScoreWithValue[V]*): F[Long] =
    wrapper.wrap(
      "zAdd",
      args.map(Attributes.zAddArgs).toList ::: Attributes.scoresWithValue(recordValue, values).toList :::
        keyAsAttribute(key).toList
    )(cmd.zAdd(key, args, values*))

  override def zAddIncr(key: K, args: Option[ZAddArgs], value: effects.ScoreWithValue[V]): F[Double] =
    wrapper.wrap(
      "zAddIncr",
      Attributes.scoreWithValue(recordValue, value) ::: args.map(Attributes.zAddArgs).toList ::: keyAsAttribute(
        key
      ).toList
    )(cmd.zAddIncr(key, args, value))

  override def zIncrBy(key: K, member: V, amount: Double): F[Double] =
    wrapper.wrap(
      "zIncrBy",
      valueAsAttribute(member, Attributes.Member).toList ::: Attributes.AmountDouble(amount) :: keyAsAttribute(
        key
      ).toList
    )(
      cmd.zIncrBy(key, member, amount)
    )

  override def zInterStore(destination: K, args: Option[ZStoreArgs], keys: K*): F[Long] =
    wrapper.wrap(
      "zInterStore",
      keyAsAttribute(destination, Attributes.Destination).toList ::: args
        .map(Attributes.zStoreArgs)
        .toList ::: keys2AsAttribute(keys).toList
    )(cmd.zInterStore(destination, args, keys*))

  override def zRem(key: K, values: V*): F[Long] =
    wrapper.wrap("zRem", values2AsAttribute(values).toList ::: keyAsAttribute(key).toList)(cmd.zRem(key, values*))

  override def zRemRangeByLex(key: K, range: effects.ZRange[V]): F[Long] =
    wrapper.wrap(
      "zRemRangeByLex",
      recordValue.toList.flatMap(f => Attributes.range(range.map(f))) ::: keyAsAttribute(key).toList
    )(
      cmd.zRemRangeByLex(key, range)
    )

  override def zRemRangeByRank(key: K, start: Long, stop: Long): F[Long] =
    wrapper.wrap("zRemRangeByRank", Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList)(
      cmd.zRemRangeByRank(key, start, stop)
    )

  override def zRemRangeByScore[T: Numeric](key: K, range: effects.ZRange[T]): F[Long] =
    wrapper.wrap("zRemRangeByScore", Attributes.range(range.map(_.toString)) ::: keyAsAttribute(key).toList)(
      cmd.zRemRangeByScore(key, range)
    )

  override def zUnionStore(destination: K, args: Option[ZStoreArgs], keys: K*): F[Long] =
    wrapper.wrap(
      "zUnionStore",
      keyAsAttribute(destination, Attributes.Destination).toList ::: args
        .map(Attributes.zStoreArgs)
        .toList ::: keys2AsAttribute(keys).toList
    )(cmd.zUnionStore(destination, args, keys*))

  override def blPop(timeout: Duration, keys: NonEmptyList[K]): F[Option[(K, V)]] =
    wrapper.wrap("blPop", Attributes.timeout(timeout) :: keys2AsAttribute(keys.toList).toList)(cmd.blPop(timeout, keys))

  override def brPop(timeout: Duration, keys: NonEmptyList[K]): F[Option[(K, V)]] =
    wrapper.wrap("brPop", Attributes.timeout(timeout) :: keys2AsAttribute(keys.toList).toList)(cmd.brPop(timeout, keys))

  override def brPopLPush(timeout: Duration, source: K, destination: K): F[Option[V]] =
    wrapper.wrap(
      "brPopLPush",
      Attributes.timeout(timeout) :: keyAsAttribute(source, Attributes.Source).toList ::: keyAsAttribute(
        destination,
        Attributes.Destination
      ).toList
    )(
      cmd.brPopLPush(timeout, source, destination)
    )

  override def lIndex(key: K, index: Long): F[Option[V]] =
    wrapper.wrap("lIndex", Attributes.Index(index) :: keyAsAttribute(key).toList)(cmd.lIndex(key, index))

  override def lLen(key: K): F[Long] =
    wrapper.wrap("lLen", keyAsAttribute(key).toList)(cmd.lLen(key))

  override def lRange(key: K, start: Long, stop: Long): F[List[V]] =
    wrapper.wrap("lRange", Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList)(
      cmd.lRange(key, start, stop)
    )

  override def lInsertAfter(key: K, pivot: V, value: V): F[Long] =
    wrapper.wrap(
      "lInsertAfter",
      kvAsAttributes(key, value).toList ::: valueAsAttribute(value, Attributes.Pivot).toList
    )(
      cmd.lInsertAfter(key, pivot, value)
    )

  override def lInsertBefore(key: K, pivot: V, value: V): F[Long] =
    wrapper.wrap(
      "lInsertBefore",
      kvAsAttributes(key, value).toList ::: valueAsAttribute(value, Attributes.Pivot).toList
    )(
      cmd.lInsertBefore(key, pivot, value)
    )

  override def lRem(key: K, count: Long, value: V): F[Long] =
    wrapper.wrap("lRem", Attributes.Count(count) :: kvAsAttributes(key, value).toList)(cmd.lRem(key, count, value))

  override def lSet(key: K, index: Long, value: V): F[Unit] =
    wrapper.wrap("lSet", Attributes.Index(index) :: kvAsAttributes(key, value).toList)(cmd.lSet(key, index, value))

  override def lTrim(key: K, start: Long, stop: Long): F[Unit] =
    wrapper.wrap("lTrim", Attributes.Start(start) :: Attributes.Stop(stop) :: keyAsAttribute(key).toList)(
      cmd.lTrim(key, start, stop)
    )

  override def lPop(key: K): F[Option[V]] =
    wrapper.wrap("lPop", keyAsAttribute(key).toList)(cmd.lPop(key))

  override def lPush(key: K, values: V*): F[Long] =
    wrapper.wrap("lPush", keyAsAttribute(key).toList ::: values2AsAttribute(values).toList)(cmd.lPush(key, values*))

  override def lPushX(key: K, values: V*): F[Long] =
    wrapper.wrap("lPushX", keyAsAttribute(key).toList ::: values2AsAttribute(values).toList)(cmd.lPushX(key, values*))

  override def rPop(key: K): F[Option[V]] =
    wrapper.wrap("rPop", keyAsAttribute(key).toList)(cmd.rPop(key))

  override def rPopLPush(source: K, destination: K): F[Option[V]] =
    wrapper.wrap(
      "rPopLPush",
      keyAsAttribute(source, Attributes.Source).toList ::: keyAsAttribute(destination, Attributes.Destination).toList
    )(
      cmd.rPopLPush(source, destination)
    )

  override def rPush(key: K, values: V*): F[Long] =
    wrapper.wrap("rPush", keyAsAttribute(key).toList ::: values2AsAttribute(values).toList)(cmd.rPush(key, values*))

  override def rPushX(key: K, values: V*): F[Long] =
    wrapper.wrap("rPushX", keyAsAttribute(key).toList ::: values2AsAttribute(values).toList)(cmd.rPushX(key, values*))

  override def geoDist(key: K, from: V, to: V, unit: GeoArgs.Unit): F[Double] =
    wrapper.wrap(
      "geoDist",
      Attributes.geoUnit(unit) :: keyAsAttribute(key).toList ::: valueAsAttribute(
        from,
        Attributes.From
      ).toList ::: valueAsAttribute(to, Attributes.To).toList
    )(
      cmd.geoDist(key, from, to, unit)
    )

  override def geoHash(key: K, values: V*): F[List[Option[String]]] =
    wrapper.wrap("geoHash", keyAsAttribute(key).toList ::: values2AsAttribute(values).toList)(cmd.geoHash(key, values*))

  override def geoPos(key: K, values: V*): F[List[effects.GeoCoordinate]] =
    wrapper.wrap("geoPos", keyAsAttribute(key).toList ::: values2AsAttribute(values).toList)(cmd.geoPos(key, values*))

  override def geoRadius(key: K, geoRadius: effects.GeoRadius, unit: GeoArgs.Unit): F[Set[V]] =
    wrapper.wrap(
      "geoRadius",
      Attributes.geoRadius(geoRadius) ::: Attributes.geoUnit(unit) :: keyAsAttribute(key).toList
    )(
      cmd.geoRadius(key, geoRadius, unit)
    )

  override def geoRadius(
      key: K,
      geoRadius: effects.GeoRadius,
      unit: GeoArgs.Unit,
      args: GeoArgs
  ): F[List[effects.GeoRadiusResult[V]]] =
    wrapper.wrap(
      "geoRadius",
      Attributes.geoRadius(geoRadius) ::: Attributes.geoUnit(unit) :: keyAsAttribute(key).toList
    )(
      cmd.geoRadius(key, geoRadius, unit, args)
    )

  override def geoRadiusByMember(key: K, value: V, dist: effects.Distance, unit: GeoArgs.Unit): F[Set[V]] =
    wrapper.wrap(
      "geoRadiusByMember",
      Attributes.distance(dist) :: Attributes
        .geoUnit(unit) :: keyAsAttribute(key).toList ::: valueAsAttribute(value).toList
    )(
      cmd.geoRadiusByMember(key, value, dist, unit)
    )

  override def geoRadiusByMember(
      key: K,
      value: V,
      dist: effects.Distance,
      unit: GeoArgs.Unit,
      args: GeoArgs
  ): F[List[effects.GeoRadiusResult[V]]] =
    wrapper.wrap(
      "geoRadiusByMember",
      Attributes.geoArgs(args) :: Attributes.distance(dist) :: Attributes.geoUnit(unit) ::
        keyAsAttribute(key).toList ::: valueAsAttribute(value).toList
    )(
      cmd.geoRadiusByMember(key, value, dist, unit, args)
    )

  override def geoAdd(key: K, geoValues: effects.GeoLocation[V]*): F[Unit] =
    wrapper.wrap(
      "geoAdd",
      keyAsAttribute(key).toList ::: recordValue.map(f => Attributes.GeoValues(geoValues.map(_.map(f).show))).toList
    )(
      cmd.geoAdd(key, geoValues*)
    )

  override def geoRadius(
      key: K,
      geoRadius: effects.GeoRadius,
      unit: GeoArgs.Unit,
      storage: effects.GeoRadiusKeyStorage[K]
  ): F[Unit] =
    wrapper.wrap(
      "geoRadius",
      Attributes.geoRadius(geoRadius) ::: Attributes.geoUnit(unit) :: keyAsAttribute(key).toList :::
        recordKey.map(storage.map).toList.flatMap(Attributes.geoRadiusKeyStorage)
    )(
      cmd.geoRadius(key, geoRadius, unit, storage)
    )

  override def geoRadius(
      key: K,
      geoRadius: effects.GeoRadius,
      unit: GeoArgs.Unit,
      storage: effects.GeoRadiusDistStorage[K]
  ): F[Unit] = wrapper.wrap(
    "geoRadius",
    Attributes.geoRadius(geoRadius) ::: Attributes.geoUnit(unit) :: keyAsAttribute(key).toList :::
      recordKey.map(storage.map).toList.flatMap(Attributes.geoRadiusDistStorage)
  )(cmd.geoRadius(key, geoRadius, unit, storage))

  override def geoRadiusByMember(
      key: K,
      value: V,
      dist: effects.Distance,
      unit: GeoArgs.Unit,
      storage: effects.GeoRadiusKeyStorage[K]
  ): F[Unit] = wrapper.wrap(
    "geoRadiusByMember",
    Attributes.distance(dist) :: Attributes.geoUnit(unit) :: keyAsAttribute(key).toList :::
      valueAsAttribute(value).toList :::
      recordKey.map(storage.map).toList.flatMap(Attributes.geoRadiusKeyStorage)
  )(
    cmd.geoRadiusByMember(key, value, dist, unit, storage)
  )

  override def geoRadiusByMember(
      key: K,
      value: V,
      dist: effects.Distance,
      unit: GeoArgs.Unit,
      storage: effects.GeoRadiusDistStorage[K]
  ): F[Unit] =
    wrapper.wrap(
      "geoRadiusByMember",
      Attributes.distance(dist) :: Attributes.geoUnit(unit) :: keyAsAttribute(key).toList :::
        valueAsAttribute(value).toList :::
        recordKey.map(storage.map).toList.flatMap(Attributes.geoRadiusDistStorage)
    )(
      cmd.geoRadiusByMember(key, value, dist, unit, storage)
    )

  override def ping: F[String] =
    wrapper.wrap("ping")(cmd.ping)

  override def select(index: Int): F[Unit] =
    wrapper.wrap("select", Attributes.Index(index.toLong) :: Nil)(cmd.select(index))

  override def auth(password: CharSequence): F[Boolean] =
    wrapper.wrap("auth (password)", Nil)(cmd.auth(password))

  override def auth(username: String, password: CharSequence): F[Boolean] =
    wrapper.wrap("auth (username & password)", Nil)(cmd.auth(username, password))

  override def setClientName(name: K): F[Boolean] =
    wrapper.wrap("setClientName", keyAsAttribute(name, Attributes.Name).toList)(cmd.setClientName(name))

  override def getClientName(): F[Option[K]] =
    wrapper.wrap("getClientName")(cmd.getClientName())

  override def getClientId(): F[Long] =
    wrapper.wrap("getClientId")(cmd.getClientId())

  override def getClientInfo: F[Map[String, String]] =
    wrapper.wrap("getClientInfo")(cmd.getClientInfo)

  override def setLibName(name: String): F[Boolean] =
    wrapper.wrap("setLibName", Attributes.Name(name) :: Nil)(cmd.setLibName(name))

  override def setLibVersion(version: String): F[Boolean] =
    wrapper.wrap("setLibVersion", Attributes.Version(version) :: Nil)(cmd.setLibVersion(version))

  override def keys(key: K): F[List[K]] =
    wrapper.wrap("keys", keyAsAttribute(key).toList)(cmd.keys(key))

  override def flushAll: F[Unit] =
    wrapper.wrap("flushAll")(cmd.flushAll)

  override def flushAll(mode: effects.FlushMode): F[Unit] =
    wrapper.wrap("flushAll", Attributes.FlushMode(mode.show) :: Nil)(cmd.flushAll(mode))

  override def flushDb: F[Unit] =
    wrapper.wrap("flushDb")(cmd.flushDb)

  override def flushDb(mode: effects.FlushMode): F[Unit] =
    wrapper.wrap("flushDb", Attributes.FlushMode(mode.show) :: Nil)(cmd.flushDb(mode))

  override def info: F[Map[String, String]] =
    wrapper.wrap("info")(cmd.info)

  override def info(section: String): F[Map[String, String]] =
    wrapper.wrap("info", Attributes.Section(section) :: Nil)(cmd.info(section))

  override def dbsize: F[Long] =
    wrapper.wrap("dbsize")(cmd.dbsize)

  override def lastSave: F[Instant] =
    wrapper.wrap("lastSave")(cmd.lastSave)

  override def slowLogLen: F[Long] =
    wrapper.wrap("slowLogLen")(cmd.slowLogLen)

  override def multi: F[Unit] =
    wrapper.wrap("multi")(cmd.multi)

  override def exec: F[Unit] =
    wrapper.wrap("exec")(cmd.exec)

  override def discard: F[Unit] =
    wrapper.wrap("discard")(cmd.discard)

  override def watch(keys: K*): F[Unit] =
    wrapper.wrap("watch", keys2AsAttribute(keys).toList)(cmd.watch(keys: _*))

  override def unwatch: F[Unit] =
    wrapper.wrap("unwatch")(cmd.unwatch)

  override def transact[A](fs: TxStore[F, String, A] => List[F[Unit]]): F[Map[String, A]] =
    wrapper.wrap("transact")(cmd.transact(fs))

  override def transact_(fs: List[F[Unit]]): F[Unit] =
    wrapper.wrap("transact_", Attributes.EffectCount(fs.length.toLong) :: Nil)(cmd.transact_(fs))

  override def pipeline[A](fs: TxStore[F, String, A] => List[F[Unit]]): F[Map[String, A]] =
    wrapper.wrap("pipeline")(cmd.pipeline(fs))

  override def pipeline_(fs: List[F[Unit]]): F[Unit] =
    wrapper.wrap("pipeline_", Attributes.EffectCount(fs.length.toLong) :: Nil)(cmd.pipeline_(fs))

  override def enableAutoFlush: F[Unit] =
    wrapper.wrap("enableAutoFlush")(cmd.enableAutoFlush)

  override def disableAutoFlush: F[Unit] =
    wrapper.wrap("disableAutoFlush")(cmd.disableAutoFlush)

  override def flushCommands: F[Unit] =
    wrapper.wrap("flushCommands")(cmd.flushCommands)

  override def eval(script: String, output: effects.ScriptOutputType[V]): F[output.R] =
    wrapper.wrap("eval", Nil)(cmd.eval(script, output))

  override def eval(script: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] =
    wrapper.wrap("eval", keys2AsAttribute(keys).toList)(cmd.eval(script, output, keys))

  override def eval(script: String, output: effects.ScriptOutputType[V], keys: List[K], values: List[V]): F[output.R] =
    wrapper.wrap("eval", keys2AsAttribute(keys).toList ++ values2AsAttribute(values).toList)(
      cmd.eval(script, output, keys, values)
    )

  override def evalReadOnly(script: String, output: effects.ScriptOutputType[V]): F[output.R] =
    wrapper.wrap("evalReadOnly", Nil)(cmd.evalReadOnly(script, output))

  override def evalReadOnly(script: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] =
    wrapper.wrap("evalReadOnly", keys2AsAttribute(keys).toList)(cmd.evalReadOnly(script, output, keys))

  override def evalReadOnly(
      script: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] =
    wrapper.wrap("evalReadOnly", keys2AsAttribute(keys).toList ++ values2AsAttribute(values).toList)(
      cmd.evalReadOnly(script, output, keys, values)
    )

  override def evalSha(digest: String, output: effects.ScriptOutputType[V]): F[output.R] =
    wrapper.wrap("evalSha", Attributes.Digest(digest) :: Nil)(cmd.evalSha(digest, output))

  override def evalSha(digest: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] =
    wrapper.wrap("evalSha", Attributes.Digest(digest) :: keys2AsAttribute(keys).toList)(
      cmd.evalSha(digest, output, keys)
    )

  override def evalSha(
      digest: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] =
    wrapper.wrap(
      "evalSha",
      Attributes.Digest(digest) :: keys2AsAttribute(keys).toList ++ values2AsAttribute(values).toList
    )(
      cmd.evalSha(digest, output, keys, values)
    )

  override def evalShaReadOnly(digest: String, output: effects.ScriptOutputType[V]): F[output.R] =
    wrapper.wrap("evalShaReadOnly", Attributes.Digest(digest) :: Nil)(cmd.evalShaReadOnly(digest, output))

  override def evalShaReadOnly(digest: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] =
    wrapper.wrap("evalShaReadOnly", Attributes.Digest(digest) :: keys2AsAttribute(keys).toList)(
      cmd.evalShaReadOnly(digest, output, keys)
    )

  override def evalShaReadOnly(
      digest: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] =
    wrapper.wrap(
      "evalShaReadOnly",
      Attributes.Digest(digest) :: keys2AsAttribute(keys).toList ++ values2AsAttribute(values).toList
    )(
      cmd.evalShaReadOnly(digest, output, keys, values)
    )

  override def scriptLoad(script: String): F[String] =
    wrapper.wrap("scriptLoad")(cmd.scriptLoad(script))

  override def scriptLoad(script: Array[Byte]): F[String] =
    wrapper.wrap("scriptLoad")(cmd.scriptLoad(script))

  override def scriptExists(digests: String*): F[List[Boolean]] =
    wrapper.wrap("scriptExists", Attributes.Digests(digests) :: Nil)(cmd.scriptExists(digests*))

  override def scriptFlush: F[Unit] =
    wrapper.wrap("scriptFlush")(cmd.scriptFlush)

  override def digest(script: String): F[String] =
    wrapper.wrap("digest")(cmd.digest(script))

  override def fcall(function: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] =
    wrapper.wrap("fcall", Attributes.Function(function) :: keys2AsAttribute(keys).toList)(
      cmd.fcall(function, output, keys)
    )

  override def fcall(
      function: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] =
    wrapper.wrap(
      "fcall",
      Attributes.Function(function) :: keys2AsAttribute(keys).toList ++ values2AsAttribute(values).toList
    )(
      cmd.fcall(function, output, keys, values)
    )

  override def fcallReadOnly(function: String, output: effects.ScriptOutputType[V], keys: List[K]): F[output.R] =
    wrapper.wrap("fcallReadOnly", Attributes.Function(function) :: keys2AsAttribute(keys).toList)(
      cmd.fcallReadOnly(function, output, keys)
    )

  override def fcallReadOnly(
      function: String,
      output: effects.ScriptOutputType[V],
      keys: List[K],
      values: List[V]
  ): F[output.R] =
    wrapper.wrap(
      "fcallReadOnly",
      Attributes.Function(function) :: keys2AsAttribute(keys).toList ++ values2AsAttribute(values).toList
    )(
      cmd.fcallReadOnly(function, output, keys, values)
    )

  override def functionLoad(functionCode: String): F[String] =
    wrapper.wrap("functionLoad")(cmd.functionLoad(functionCode))

  override def functionLoad(functionCode: String, replace: Boolean): F[String] =
    wrapper.wrap("functionLoad", Attributes.Replace(replace) :: Nil)(cmd.functionLoad(functionCode, replace))

  override def functionDump(): F[Array[Byte]] =
    wrapper.wrap("functionDump")(cmd.functionDump())

  override def functionRestore(dump: Array[Byte]): F[String] =
    wrapper.wrap("functionRestore")(cmd.functionRestore(dump))

  override def functionRestore(dump: Array[Byte], mode: effects.FunctionRestoreMode): F[String] =
    wrapper.wrap("functionRestore", Attributes.FunctionRestoreMode(mode.show) :: Nil)(cmd.functionRestore(dump, mode))

  override def functionFlush(flushMode: effects.FlushMode): F[String] =
    wrapper.wrap("functionFlush", Attributes.FlushMode(flushMode.show) :: Nil)(cmd.functionFlush(flushMode))

  override def functionKill(): F[String] =
    wrapper.wrap("functionKill")(cmd.functionKill())

  override def functionList(): F[List[Map[String, Any]]] =
    wrapper.wrap("functionList")(cmd.functionList())

  override def functionList(libraryName: String): F[List[Map[String, Any]]] =
    wrapper.wrap("functionList", Attributes.LibraryName(libraryName) :: Nil)(cmd.functionList(libraryName))

  override def copy(source: K, destination: K): F[Boolean] =
    wrapper.wrap(
      "copy",
      keyAsAttribute(source, Attributes.Source).toList ++ keyAsAttribute(destination, Attributes.Destination).toList
    )(cmd.copy(source, destination))

  override def copy(source: K, destination: K, copyArgs: effects.CopyArgs): F[Boolean] =
    wrapper.wrap(
      "copy",
      keyAsAttribute(source, Attributes.Source).toList ::: keyAsAttribute(
        destination,
        Attributes.Destination
      ).toList ::: Attributes.copyArgs(copyArgs)
    )(
      cmd.copy(source, destination, copyArgs)
    )

  override def del(key: K*): F[Long] =
    wrapper.wrap("del", keys2AsAttribute(key).toList)(cmd.del(key*))

  override def dump(key: K): F[Option[Array[Byte]]] =
    wrapper.wrap("dump", keyAsAttribute(key).toList)(cmd.dump(key))

  override def exists(key: K*): F[Boolean] =
    wrapper.wrap("exists", keys2AsAttribute(key).toList)(cmd.exists(key*))

  override def expire(key: K, expiresIn: FiniteDuration): F[Boolean] =
    wrapper.wrap("expire", Attributes.expiresIn(expiresIn) :: keyAsAttribute(key).toList)(cmd.expire(key, expiresIn))

  override def expire(key: K, expiresIn: FiniteDuration, expireExistenceArg: effects.ExpireExistenceArg): F[Boolean] =
    wrapper.wrap(
      "expire",
      Attributes.expireExistenceArg(expireExistenceArg) :: Attributes.expiresIn(expiresIn) :: keyAsAttribute(key).toList
    )(cmd.expire(key, expiresIn, expireExistenceArg))

  override def expireAt(key: K, at: Instant): F[Boolean] =
    wrapper.wrap("expireAt", Attributes.at(at) :: keyAsAttribute(key).toList)(cmd.expireAt(key, at))

  override def expireAt(key: K, at: Instant, expireExistenceArg: effects.ExpireExistenceArg): F[Boolean] =
    wrapper.wrap(
      "expireAt",
      Attributes.expireExistenceArg(expireExistenceArg) :: Attributes.at(at) :: keyAsAttribute(key).toList
    )(cmd.expireAt(key, at, expireExistenceArg))

  override def objectIdletime(key: K): F[Option[FiniteDuration]] =
    wrapper.wrap("objectIdletime", keyAsAttribute(key).toList)(cmd.objectIdletime(key))

  override def persist(key: K): F[Boolean] =
    wrapper.wrap("persist", keyAsAttribute(key).toList)(cmd.persist(key))

  override def pttl(key: K): F[Option[FiniteDuration]] =
    wrapper.wrap("pttl", keyAsAttribute(key).toList)(cmd.pttl(key))

  override def randomKey: F[Option[K]] =
    wrapper.wrap("randomKey", Nil)(cmd.randomKey)

  override def restore(key: K, value: Array[Byte]): F[Unit] =
    wrapper.wrap("restore", keyAsAttribute(key).toList)(cmd.restore(key, value))

  override def restore(key: K, value: Array[Byte], restoreArgs: effects.RestoreArgs): F[Unit] =
    wrapper.wrap("restore", keyAsAttribute(key).toList ::: Attributes.restoreArgs(restoreArgs))(
      cmd.restore(key, value, restoreArgs)
    )

  override def scan: F[data.KeyScanCursor[K]] =
    wrapper.wrap("scan", Nil)(cmd.scan)

  override def scan(cursor: Long): F[data.KeyScanCursor[K]] =
    wrapper.wrap("scan", Attributes.Cursor(cursor) :: Nil)(cmd.scan(cursor))

  override def scan(previous: data.KeyScanCursor[K]): F[data.KeyScanCursor[K]] =
    wrapper.wrap("scan", mapAsAttribute(previous, recordKey, Attributes.Previous).toList)(cmd.scan(previous))

  override def scan(scanArgs: effects.ScanArgs): F[data.KeyScanCursor[K]] =
    wrapper.wrap("scan", Attributes.ScanArgs(scanArgs.show) :: Nil)(cmd.scan(scanArgs))

  override def scan(keyScanArgs: effects.KeyScanArgs): F[data.KeyScanCursor[K]] =
    wrapper.wrap("scan", Attributes.KeyScanArgs(keyScanArgs.show) :: Nil)(cmd.scan(keyScanArgs))

  override def scan(cursor: Long, scanArgs: effects.ScanArgs): F[data.KeyScanCursor[K]] =
    wrapper.wrap("scan", Attributes.Cursor(cursor) :: Attributes.ScanArgs(scanArgs.show) :: Nil)(
      cmd.scan(cursor, scanArgs)
    )

  override def scan(previous: data.KeyScanCursor[K], scanArgs: effects.ScanArgs): F[data.KeyScanCursor[K]] =
    wrapper.wrap(
      "scan",
      mapAsAttribute(previous, recordKey, Attributes.Previous).toList ::: Attributes.ScanArgs(scanArgs.show) :: Nil
    )(cmd.scan(previous, scanArgs))

  override def scan(cursor: data.KeyScanCursor[K], keyScanArgs: effects.KeyScanArgs): F[data.KeyScanCursor[K]] =
    wrapper.wrap(
      "scan",
      mapAsAttribute(cursor, recordKey, Attributes.CursorAsKeyScanCursor).toList ::: Attributes.KeyScanArgs(
        keyScanArgs.show
      ) :: Nil
    )(cmd.scan(cursor, keyScanArgs))

  override def typeOf(key: K): F[Option[effects.RedisType]] =
    wrapper.wrap("typeOf", keyAsAttribute(key).toList)(cmd.typeOf(key))

  override def ttl(key: K): F[Option[FiniteDuration]] =
    wrapper.wrap("ttl", keyAsAttribute(key).toList)(cmd.ttl(key))

  override def unlink(key: K*): F[Long] =
    wrapper.wrap("unlink", keys2AsAttribute(key).toList)(cmd.unlink(key*))

  override def pfAdd(key: K, values: V*): F[Long] =
    wrapper.wrap("pfAdd", keyAsAttribute(key).toList ::: values2AsAttribute(values).toList)(cmd.pfAdd(key, values*))

  override def pfCount(key: K): F[Long] =
    wrapper.wrap("pfCount", keyAsAttribute(key).toList)(cmd.pfCount(key))

  override def pfMerge(outputKey: K, inputKeys: K*): F[Unit] =
    wrapper.wrap(
      "pfMerge",
      keyAsAttribute(outputKey, Attributes.OutputKey).toList ::: keys2AsAttribute(
        inputKeys,
        Attributes.InputKeys
      ).toList
    )(
      cmd.pfMerge(outputKey, inputKeys*)
    )

  override def bitCount(key: K): F[Long] =
    wrapper.wrap("bitCount", keyAsAttribute(key).toList)(cmd.bitCount(key))

  override def bitCount(key: K, start: Long, end: Long): F[Long] =
    wrapper.wrap("bitCount", Attributes.Start(start) :: Attributes.End(end) :: keyAsAttribute(key).toList)(
      cmd.bitCount(key, start, end)
    )

  override def bitField(key: K, operations: BitCommandOperation*): F[List[Long]] =
    wrapper.wrap("bitField", Attributes.operations(operations*) :: keyAsAttribute(key).toList)(
      cmd.bitField(key, operations*)
    )

  override def bitOpAnd(destination: K, sources: K*): F[Unit] =
    wrapper.wrap(
      "bitOpAnd",
      keys2AsAttribute(sources).toList ::: keyAsAttribute(destination, Attributes.Destination).toList
    )(
      cmd.bitOpAnd(destination, sources*)
    )

  override def bitOpNot(destination: K, source: K): F[Unit] =
    wrapper.wrap(
      "bitOpNot",
      keyAsAttribute(destination, Attributes.Destination).toList ::: keyAsAttribute(source).toList
    )(
      cmd.bitOpNot(destination, source)
    )

  override def bitOpOr(destination: K, sources: K*): F[Unit] =
    wrapper.wrap(
      "bitOpOr",
      keys2AsAttribute(sources).toList ::: keyAsAttribute(destination, Attributes.Destination).toList
    )(
      cmd.bitOpOr(destination, sources*)
    )

  override def bitOpXor(destination: K, sources: K*): F[Unit] =
    wrapper.wrap(
      "bitOpXor",
      keys2AsAttribute(sources).toList ::: keyAsAttribute(destination, Attributes.Destination).toList
    )(
      cmd.bitOpXor(destination, sources*)
    )

  override def bitPos(key: K, state: Boolean): F[Long] =
    wrapper.wrap("bitPos", Attributes.State(state) :: keyAsAttribute(key).toList)(
      cmd.bitPos(key, state)
    )

  override def bitPos(key: K, state: Boolean, start: Long): F[Long] =
    wrapper.wrap(
      "bitPos",
      Attributes.Start(start) :: Attributes.State(state) :: keyAsAttribute(key).toList
    )(
      cmd.bitPos(key, state, start)
    )

  override def bitPos(key: K, state: Boolean, start: Long, end: Long): F[Long] =
    wrapper.wrap(
      "bitPos",
      Attributes.Start(start) :: Attributes.End(end) :: Attributes.State(state) :: keyAsAttribute(key).toList
    )(
      cmd.bitPos(key, state, start, end)
    )

  override def getBit(key: K, offset: Long): F[Option[Long]] =
    wrapper.wrap("getBit", Attributes.Offset(offset) :: keyAsAttribute(key).toList)(
      cmd.getBit(key, offset)
    )

  override def setBit(key: K, offset: Long, value: Int): F[Long] =
    wrapper.wrap(
      "setBit",
      Attributes.Offset(offset) :: Attributes.ValueLong(value.toLong) :: keyAsAttribute(key).toList
    )(
      cmd.setBit(key, offset, value)
    )
}
