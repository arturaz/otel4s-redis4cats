package dev.profunktor.redis4cats.otel4s

import org.typelevel.otel4s.AttributeKey
import org.typelevel.otel4s.Attribute

trait CoreHelpers[K, V] {
  import CoreImplicits.*
  private def Attributes = CoreAttributes

  def recordKey: Option[K => String]
  def recordValue: Option[V => String]

  def mapToString[Value[_], A](value: Value[A], mapper: Option[A => String])(implicit
      toString: ToString[Value]
  ): Option[String] =
    mapper.map(toString(value, _))

  def mapToStrings[Value[_], A](value: Value[A], mapper: Option[A => String])(implicit
      toString: ToStrings[Value]
  ): Option[Seq[String]] =
    mapper.map(toString(value, _))

  def mapAsAttribute[Value[_], A](
      value: Value[A],
      mapper: Option[A => String],
      attr: AttributeKey[Seq[String]]
  )(implicit toStrings: ToStrings[Value]): Option[Attribute[Seq[String]]] =
    mapToStrings(value, mapper).map(attr(_))

  def maybeMappableValueAsAttribute[A](
      maybeMapper: Option[A => String],
      value: A,
      attr: AttributeKey[String]
  ): Option[Attribute[String]] =
    maybeMapper match {
      case None         => None
      case Some(mapper) => Some(attr(mapper(value)))
    }

  def maybeMappableValuesAsAttribute[A](
      maybeMapper: Option[A => String],
      value: A,
      others: IterableOnce[A],
      attr: AttributeKey[Seq[String]]
  ): Option[Attribute[Seq[String]]] =
    maybeMapper match {
      case None         => None
      case Some(mapper) => Some(attr((Iterator(value) ++ others.iterator).map(mapper).toSeq))
    }

  def maybeMappableValuesAsAttribute[A](
      maybeMapper: Option[A => String],
      values: IterableOnce[A],
      attr: AttributeKey[Seq[String]]
  ): Option[Attribute[Seq[String]]] =
    maybeMapper match {
      case None         => None
      case Some(mapper) => Some(attr(values.iterator.map(mapper).toSeq))
    }

  def keyAsAttribute(key: K, attr: AttributeKey[String] = Attributes.Key): Option[Attribute[String]] =
    maybeMappableValueAsAttribute(recordKey, key, attr)

  def keysAsAttribute(
      key: K,
      others: IterableOnce[K],
      attr: AttributeKey[Seq[String]] = Attributes.Keys
  ): Option[Attribute[Seq[String]]] =
    maybeMappableValuesAsAttribute(recordKey, key, others, attr)

  def keys2AsAttribute(
      keys: IterableOnce[K],
      attr: AttributeKey[Seq[String]] = Attributes.Keys
  ): Option[Attribute[Seq[String]]] =
    maybeMappableValuesAsAttribute(recordKey, keys, attr)

  def valueAsAttribute(value: V, attr: AttributeKey[String] = Attributes.Value): Option[Attribute[String]] =
    maybeMappableValueAsAttribute(recordValue, value, attr)

  def valuesAsAttribute(
      value: V,
      others: IterableOnce[V],
      attr: AttributeKey[Seq[String]] = Attributes.Values
  ): Option[Attribute[Seq[String]]] =
    maybeMappableValuesAsAttribute(recordValue, value, others, attr)

  def values2AsAttribute(
      values: IterableOnce[V],
      attr: AttributeKey[Seq[String]] = Attributes.Values
  ): Option[Attribute[Seq[String]]] =
    maybeMappableValuesAsAttribute(recordValue, values, attr)

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
}
