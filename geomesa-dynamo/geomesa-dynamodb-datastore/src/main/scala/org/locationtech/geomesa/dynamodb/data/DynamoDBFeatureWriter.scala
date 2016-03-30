/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.dynamodbv2.document.{Item, PrimaryKey, Table}
import com.google.common.primitives.{Bytes, Ints, Longs}
import com.vividsolutions.jts.geom.Point
import org.geotools.data.simple.SimpleFeatureWriter
import org.joda.time.DateTime
import org.locationtech.geomesa.dynamo.core.DynamoPrimaryKey
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

trait DynamoDBPutter {
  // Allows impl changes between appending and updating writers
  protected def dynamoDBPut(t: Table, i: Item): Unit = ???
}

trait DynamoDBItemSerialization {
  import java.{lang => jl}

  val jlBool = classOf[jl.Boolean]
  val jlShort = classOf[jl.Short]
  val jlInteger = classOf[jl.Integer]
  val jlLong = classOf[jl.Long]
  val jlFloat = classOf[jl.Float]
  val jlDouble = classOf[jl.Double]
  val jlString = classOf[jl.String]
  val jlUUID = classOf[UUID]
  val jlDate = classOf[java.util.Date]
  val vsPoint = classOf[com.vividsolutions.jts.geom.Point]

  def getSerializer(desc: AttributeDescriptor): (Item, AnyRef) => Unit = {
    val name = desc.getLocalName
    desc.getType.getBinding match {
      case b if b.equals(jlBool)    => (item: Item, attr: AnyRef) => item.withBoolean(name, attr.asInstanceOf[jl.Boolean])
      case s if s.equals(jlShort)   => (item: Item, attr: AnyRef) => item.withShort(name,   attr.asInstanceOf[jl.Short])
      case i if i.equals(jlInteger) => (item: Item, attr: AnyRef) => item.withInt(name,     attr.asInstanceOf[jl.Integer])
      case l if l.equals(jlLong)    => (item: Item, attr: AnyRef) => item.withLong(name,    attr.asInstanceOf[jl.Long])
      case f if f.equals(jlFloat)   => (item: Item, attr: AnyRef) => item.withFloat(name,   attr.asInstanceOf[jl.Float])
      case d if d.equals(jlDouble)  => (item: Item, attr: AnyRef) => item.withDouble(name,  attr.asInstanceOf[jl.Double])
      case s if s.equals(jlString)  => (item: Item, attr: AnyRef) => item.withString(name,  attr.asInstanceOf[String])
      case u if u.equals(jlUUID)    => (item: Item, attr: AnyRef) => item.withBinary(name,  encodeUUID(attr.asInstanceOf[UUID]))
      case t if t.equals(jlDate)    => (item: Item, attr: AnyRef) => item.withLong(name,    encodeDate(attr.asInstanceOf[Date]))
      case p if vsPoint.isAssignableFrom(p) => (item: Item, attr: AnyRef) => item.withBinary(name, encodeWKB(attr.asInstanceOf[Point]))
      case _ => throw new Exception(s"Could not form serializer for feature attribute: $name of type: ${desc.getType.getName.toString}")
    }
  }

  def getDeserializer(desc: AttributeDescriptor): (Item) => Object = {
    val name = desc.getLocalName
    desc.getType.getBinding match {
      case b if b.equals(jlBool)    => item: Item => item.getBoolean(name).asInstanceOf[Object]
      case s if s.equals(jlShort)   => item: Item => item.getShort(name).asInstanceOf[Object]
      case i if i.equals(jlInteger) => item: Item => item.getInt(name).asInstanceOf[Object]
      case l if l.equals(jlLong)    => item: Item => item.getLong(name).asInstanceOf[Object]
      case f if f.equals(jlFloat)   => item: Item => item.getFloat(name).asInstanceOf[Object]
      case d if d.equals(jlDouble)  => item: Item => item.getDouble(name).asInstanceOf[Object]
      case s if s.equals(jlString)  => item: Item => item.getString(name)
      case u if u.equals(jlUUID)    => item: Item => decodeUUID(item.getBinary(name))
      case t if t.equals(jlDate)    => item: Item => decodeDate(item.getLong(name))
      case p if vsPoint.isAssignableFrom(p) => item: Item => decodeWKB(item.getBinary(name))
      case _ => throw new Exception(s"Could not form deserializer for feature attribute: $name of type: ${desc.getType.getName.toString}")
    }
  }

  private def encodeDate(d: Date): Long = d.getTime

  private def decodeDate(d: Long): Date = new Date(d)

  private def encodeWKB(p: Point): Array[Byte] = WKBUtils.write(p)

  private def decodeWKB(b: Array[Byte]): Point = WKBUtils.read(b).asInstanceOf[Point]

  private def encodeUUID(uuid: UUID): ByteBuffer = {
    ByteBuffer.allocate(16)
      .putLong(uuid.getMostSignificantBits)
      .putLong(uuid.getLeastSignificantBits)
      .flip.asInstanceOf[ByteBuffer]
  }

  private def decodeUUID(b: Array[Byte]): UUID = {
    val bb = ByteBuffer.wrap(b)
    new UUID(bb.getLong, bb.getLong)
  }

}

trait DynamoDBFeatureWriter extends SimpleFeatureWriter with DynamoDBPutter with DynamoDBItemSerialization {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

  val dtgIndex = sft.getDtgIndex.get
  private[this] val attributeDescriptors = sft.getAttributeDescriptors.toList
  private[this] var curFeature: SimpleFeature = null
  private[this] val serializers = attributeDescriptors.map(getSerializer)

  def sft: SimpleFeatureType

  def table: Table

  override def hasNext: Boolean = true

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature(UUID.randomUUID().toString, sft)
    curFeature
  }

  override def remove(): Unit = throw new NotImplementedError("DynamoDB feature writer is append only")

  override def close(): Unit = {}

  override def getFeatureType: SimpleFeatureType = sft

  override def write(): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions._

    val geom = curFeature.point
    val x = geom.getX
    val y = geom.getY
    val dtg = new DateTime(curFeature.getAttribute(dtgIndex).asInstanceOf[Date])

    // hash key
    val pk = DynamoPrimaryKey(dtg, x, y)
    val hash = Ints.toByteArray(pk.idx)

    // range key
    val secondsInWeek = DynamoPrimaryKey.secondsInCurrentWeek(dtg)
    val z3 = DynamoPrimaryKey.SFC3D.index(x, y, secondsInWeek)
    val z3idx = Longs.toByteArray(z3.z)

    val fid = curFeature.getID

    val range = Bytes.concat(z3idx, fid.getBytes(StandardCharsets.UTF_8))

    val primaryKey = new PrimaryKey(
      DynamoDBDataStore.geomesaKeyHash, hash,
      DynamoDBDataStore.geomesaKeyRange, range
    )

    val item = new Item().withPrimaryKey(primaryKey)

    item.withString(DynamoDBDataStore.fID, fid)

    val attributes = curFeature.getAttributes.iterator()
    val serializer = serializers.iterator
    while (attributes.hasNext && serializer.hasNext) {
      serializer.next()(item, attributes.next())
    }

    this.dynamoDBPut(table, item)
    curFeature = null
  }

}

class DynamoDBAppendingFeatureWriter(val sft: SimpleFeatureType, val table: Table)
  extends DynamoDBFeatureWriter {
  override def hasNext: Boolean = false

  override def dynamoDBPut(t: Table, i: Item): Unit = {
    val ps = new PutItemSpec()
      .withItem(i)
      .withConditionExpression(s"attribute_not_exists(${DynamoDBDataStore.geomesaKeyHash})")
    t.putItem(ps)
  }
}

class DynamoDBUpdatingFeatureWriter(val sft: SimpleFeatureType, val table: Table)
  extends DynamoDBFeatureWriter {
  override def hasNext: Boolean = false

  override def dynamoDBPut(t: Table, i: Item): Unit = {
    t.putItem(i)
  }
}


