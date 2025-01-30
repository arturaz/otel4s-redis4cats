package dev.profunktor.redis4cats.otel4s

import org.typelevel.otel4s.AttributeKey
import scala.concurrent.duration.Duration
import org.typelevel.otel4s.Attribute

trait StreamsAttributes extends CoreAttributes {
  val Body: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.body")
  val ApproxMaxlen: AttributeKey[Long] = AttributeKey.long("db.redis.approxMaxlen")
  val MinId: AttributeKey[String] = AttributeKey.string("db.redis.minId")
  val ChunkSize: AttributeKey[Long] = AttributeKey.long("db.redis.chunkSize")

  /** Blocking time in milliseconds. */
  val Block: AttributeKey[Long] = AttributeKey.long("db.redis.block")
  def block(duration: Duration): Attribute[Long] = Block(durationAsLong(duration))

  val InitialOffsets: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.initialOffsets")

  val MessageId: AttributeKey[String] = AttributeKey.string("db.redis.messageId")

  val Channel: AttributeKey[String] = AttributeKey.string("db.redis.channel")
  val Channels: AttributeKey[Seq[String]] = AttributeKey.stringSeq("db.redis.channels")
  val Pattern: AttributeKey[String] = AttributeKey.string("db.redis.pattern")
  val Data: AttributeKey[String] = AttributeKey.string("db.redis.data")
}
object StreamsAttributes extends StreamsAttributes
