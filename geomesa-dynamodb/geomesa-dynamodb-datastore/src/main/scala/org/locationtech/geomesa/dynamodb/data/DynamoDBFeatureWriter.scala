/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.nio.charset.StandardCharsets
import java.util.{Date, UUID}

import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.dynamodbv2.document.{Expected, Item, PrimaryKey, Table}
import com.google.common.primitives.{Longs, Bytes}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.simple.SimpleFeatureWriter
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

trait DynamoDBPutter {
  // Allows impl changes between appending and updating writers
  protected def dynamoDBPut(t: Table, i: Item): Unit = ???
}

trait DynamoDBFeatureWriter extends SimpleFeatureWriter with DynamoDBPutter {
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
  def sft: SimpleFeatureType
  def table: Table

  private[this] var curFeature: SimpleFeature = null

  val dtgIndex = sft.getDtgIndex.get

  private val encoder = new KryoFeatureSerializer(sft)

  private def serialize(item: Item, attr: AnyRef, desc: AttributeDescriptor) = {
    import java.{lang => jl}
    desc.getType.getBinding match {
      case c if c.equals(classOf[jl.Boolean]) =>
        item.withBoolean(desc.getLocalName, attr.asInstanceOf[jl.Boolean])

      case c if c.equals(classOf[jl.Integer]) =>
        item.withInt(desc.getLocalName, attr.asInstanceOf[jl.Integer])

      case c if c.equals(classOf[jl.Double]) =>
        item.withDouble(desc.getLocalName, attr.asInstanceOf[jl.Double])

      case c if c.equals(classOf[String]) =>
        item.withString(desc.getLocalName, attr.asInstanceOf[String])

      case c if c.equals(classOf[java.util.Date]) =>
        item.withLong(desc.getLocalName, attr.asInstanceOf[java.util.Date].getTime)

      case c if classOf[com.vividsolutions.jts.geom.Geometry].isAssignableFrom(c) =>
        item.withBinary(desc.getLocalName, WKBUtils.write(attr.asInstanceOf[Geometry]))
    }
  }

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature(UUID.randomUUID().toString, sft)
    curFeature
  }

  override def remove(): Unit = throw new NotImplementedError("DynamoDB feature writer is append only")

  override def hasNext: Boolean = true

  override def write(): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions._

    // TODO: is getting the centroid here smart?
    val geom = curFeature.geometry.getCentroid
    val x = geom.getX
    val y = geom.getY
    val dtg = new DateTime(curFeature.getAttribute(dtgIndex).asInstanceOf[Date])

    val secondsInWeek = DynamoDBPrimaryKey.secondsInCurrentWeek(dtg)
    val z2 = DynamoDBPrimaryKey.SFC2D.toIndex(x, y)
    val z2idx = Longs.toByteArray(z2)
    val z3 = DynamoDBPrimaryKey.SFC3D.index(x, y, secondsInWeek)
    val z3idx = Longs.toByteArray(z3.z)

    val hash = z3idx
    val range = Bytes.concat(z2idx, curFeature.getID.getBytes(StandardCharsets.UTF_8))

    val primaryKey = new PrimaryKey(
      DynamoDBDataStore.geomesaKeyHash, hash,
      DynamoDBDataStore.geomesaKeyRange, range
    )

    val item = new Item().withPrimaryKey(primaryKey)

    curFeature.getAttributes.zip(sft.getAttributeDescriptors).foreach { case (attr, desc) => serialize(item, attr, desc) }
    item.withBinary(DynamoDBDataStore.serId, encoder.serialize(curFeature))

    this.dynamoDBPut(table, item)
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}

}

class DynamoDBAppendingFeatureWriter(val sft: SimpleFeatureType, val table: Table) extends DynamoDBFeatureWriter {
  override def hasNext: Boolean = false

  override def dynamoDBPut(t: Table, i: Item): Unit = {
    val ps = new PutItemSpec()
      .withItem(i)
      .withExpected(new Expected(DynamoDBDataStore.geomesaKeyHash).notExist())
    t.putItem(ps)
  }
}

class DynamoDBUpdatingFeatureWriter(val sft: SimpleFeatureType, val table: Table) extends DynamoDBFeatureWriter {
  override def hasNext: Boolean = false

  override def dynamoDBPut(t: Table, i: Item): Unit = {
    t.putItem(i)
  }
}


object DynamoDBPrimaryKey {

  val SFC3D = new Z3SFC
  val SFC2D = new ZCurve2D(math.pow(2,5).toInt)

  val EPOCH = new DateTime(0)
  val ONE_WEEK_IN_SECONDS = Weeks.ONE.toStandardSeconds.getSeconds

  def epochWeeks(dtg: DateTime): Weeks = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime): Int =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - epochWeeks(dtg).getWeeks*ONE_WEEK_IN_SECONDS

  case class Key(idx: Int, x: Double, y: Double, dk: Int, z: Int)

  def unapply(idx: Int): Key = {
    val dk = idx >> 16
    val z = idx & 0x000000ff
    val (x, y) = SFC2D.toPoint(z)
    Key(idx, x, y, dk, z)
  }

  def apply(dtg: DateTime, x: Double, y: Double): Key = {
    val dk = epochWeeks(dtg).getWeeks << 16
    val z = SFC2D.toIndex(x, y).toInt
    val (rx, ry) = SFC2D.toPoint(z)
    val idx = dk + z
    Key(idx, rx, ry, dk, z)
  }

}