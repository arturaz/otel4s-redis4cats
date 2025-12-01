package dev.profunktor.redis4cats.otel4s

import dev.profunktor.redis4cats.effects.{XAddArgs, XReadOffsets, XTrimArgs}
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.AttributeKey

import scala.concurrent.duration.Duration

/** Shared helpers for stream-related attributes. */
object StreamAttributeKeys {
  val Body: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.body")
  val ApproxMaxlen: AttributeKey[Long] = AttributeKey.long("db.redis.approxMaxlen")
  val MinId: AttributeKey[String] = AttributeKey.string("db.redis.minId")
  val TrimPrecision: AttributeKey[String] = AttributeKey.string("db.redis.trim.precision")
  val NoMkStream: AttributeKey[Boolean] = AttributeKey.boolean("db.redis.nomkstream")
  val InitialOffsets: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.initialOffsets")
  val MessageId: AttributeKey[String] = AttributeKey.string("db.redis.messageId")
  val Block: AttributeKey[Long] = AttributeKey.long("db.redis.block")

  def block(duration: Duration): Attribute[Long] =
    Block(CoreAttributes.durationAsLong(duration))
}

object StreamArgAttributes {
  import StreamAttributeKeys._

  def trimArgsAttributes(args: Option[XTrimArgs]): List[Attribute[?]] =
    args.toList.flatMap {
      case XTrimArgs(strategy, precision) =>
        val strategyAttributes = strategy match {
          case XTrimArgs.Strategy.MAXLEN(threshold) => ApproxMaxlen(threshold) :: Nil
          case XTrimArgs.Strategy.MINID(id)         => MinId(id) :: Nil
        }
        val precisionAttributes = precision match {
          case XTrimArgs.Precision.Exact                 => TrimPrecision("exact") :: Nil
          case XTrimArgs.Precision.Approximate(limitOpt) =>
            TrimPrecision("approximate") :: limitOpt.map(CoreAttributes.Count(_)).toList
        }

        strategyAttributes ::: precisionAttributes
    }

  def xAddArgsAttributes(args: XAddArgs): List[Attribute[?]] =
    args match {
      case XAddArgs(nomkstream, id, xTrimArgs) =>
        (if (nomkstream) List(NoMkStream(true)) else Nil) :::
          id.map(MessageId(_)).toList :::
          trimArgsAttributes(xTrimArgs)
    }

  def offsetAttributes[K](recordKey: Option[K => String], streams: Set[XReadOffsets[K]]): List[Attribute[?]] =
    recordKey
      .map { keyMapper =>
        InitialOffsets(streams.toSeq.map(offset => s"${keyMapper(offset.key)}=${offset.offset}"))
      }
      .toList
}
