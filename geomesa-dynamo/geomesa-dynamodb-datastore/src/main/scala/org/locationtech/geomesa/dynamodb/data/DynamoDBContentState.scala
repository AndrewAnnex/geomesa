/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.document.spec.{QuerySpec, ScanSpec}
import com.amazonaws.services.dynamodbv2.document.{RangeKeyCondition, Table}
import com.amazonaws.services.dynamodbv2.model.Select
import com.google.common.primitives.{Ints, Longs}
import org.geotools.data.store.{ContentEntry, ContentState}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.dynamo.core.DynamoContentState
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class DynamoDBContentState(ent: ContentEntry, catalogTable: Table, sftTable: Table, client: AmazonDynamoDBAsyncClient)
  extends ContentState(ent) with DynamoContentState with DynamoDBItemSerialization {
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

  val sft: SimpleFeatureType = DynamoDBDataStore.getSchema(entry, catalogTable)
  val geomField = sft.getGeomField
  val attrDesc = sft.getAttributeDescriptors
  val attrNames = attrDesc.map(_.getLocalName)

  val deserializers = attrDesc.map(getDeserializer).toArray

  val table: Table = sftTable
  val builderPool = ObjectPoolFactory(getBuilder, 10)
  val dynamodbClient = client

  val ALL_ATTRIBUTES: Array[String] = Array(DynamoDBDataStore.fID) ++ attrNames

  val ALL_QUERY = new ScanSpec().withAttributesToGet(ALL_ATTRIBUTES:_*)

  def getCountOfAll: Long = sftTable.getDescription.getItemCount

  def geoTimeQuery(pkz: Int, z3min: Long, z3max: Long): QuerySpec = new QuerySpec()
    .withHashKey(DynamoDBDataStore.geomesaKeyHash, Ints.toByteArray(pkz))
    .withRangeKeyCondition(genRangeKey(z3min, z3max))
    .withAttributesToGet(ALL_ATTRIBUTES:_*)

  private def genRangeKey(z3min: Long, z3max: Long): RangeKeyCondition = {
    val minZ3 = Longs.toByteArray(z3min)
    val maxZ3 = Longs.toByteArray(z3max)
    new RangeKeyCondition(DynamoDBDataStore.geomesaKeyRange).between(minZ3, maxZ3)
  }

  def geoTimeCountQuery(pkz: Int, z3min: Long, z3max: Long): QuerySpec = new QuerySpec()
    .withHashKey(DynamoDBDataStore.geomesaKeyHash, Ints.toByteArray(pkz))
    .withRangeKeyCondition(genRangeKey(z3min, z3max))
    .withSelect(Select.COUNT)

  private def getBuilder = {
    val builder = new SimpleFeatureBuilder(sft)
    builder.setValidating(java.lang.Boolean.FALSE)
    builder
  }

}

//object DynamoDBContentState {
//  import java.{lang => jl}
//
//  val jlBool = classOf[jl.Boolean]
//  val jlShort = classOf[jl.Short]
//  val jlInteger = classOf[jl.Integer]
//  val jlLong = classOf[jl.Long]
//  val jlFloat = classOf[jl.Float]
//  val jlDouble = classOf[jl.Double]
//  val jlString = classOf[jl.String]
//  val jlUUID = classOf[UUID]
//  val jlDate = classOf[java.util.Date]
//  val vsPoint = classOf[com.vividsolutions.jts.geom.Point]
//
//  def itemToDeserializers(i: Item, sft: SimpleFeatureType): Array[Object] = {
//    val keys: List[String] = i.asMap().keySet().toList
//
//    val descr: List[AttributeDescriptor] = keys.map(sft.getDescriptor)
//
//    val funcs = descr.map(attributeDecoder)
//
//    funcs.map(f => f(i))
//
//
////
////    i.attributes().map { kv: Entry[String, Any] =>
////      val key = kv.getKey
////
////    }
////
//
//  }
//
//
//  def attributetoObject(item: Item, desc: AttributeDescriptor): Any = {
//    val name = desc.getLocalName
//    desc.getType.getBinding match {
//      case b if b.equals(jlBool)    => item.getBoolean(name)
//      case s if s.equals(jlShort)   => item.getShort(name)
//      case i if i.equals(jlInteger) => item.getInt(name)
//      case l if l.equals(jlLong)    => item.getLong(name)
//      case f if f.equals(jlFloat)   => item.getFloat(name)
//      case d if d.equals(jlDouble)  => item.getDouble(name)
//      case s if s.equals(jlString)  => item.getString(name)
//      case u if u.equals(jlUUID)    => decodeUUID(item.getBinary(name))
//      case t if t.equals(jlDate)    => decodeDate(item.getLong(name))
//      case p if vsPoint.isAssignableFrom(p) => decodeWKB(item.getBinary(name))
//    }
//  }
//
//
//  def attributeDecoder(desc: AttributeDescriptor): Item => Any = {
//    val name = desc.getLocalName
//    desc.getType.getBinding match {
//      case b if b.equals(jlBool)    => item: Item => item.getBoolean(name)
//      case s if s.equals(jlShort)   => item: Item => item.getShort(name)
//      case i if i.equals(jlInteger) => item: Item => item.getInt(name)
//      case l if l.equals(jlLong)    => item: Item => item.getLong(name)
//      case f if f.equals(jlFloat)   => item: Item => item.getFloat(name)
//      case d if d.equals(jlDouble)  => item: Item => item.getDouble(name)
//      case s if s.equals(jlString)  => item: Item => item.getString(name)
//      case u if u.equals(jlUUID)    => item: Item => decodeUUID(item.getBinary(name))
//      case t if t.equals(jlDate)    => item: Item => decodeDate(item.getLong(name))
//      case p if vsPoint.isAssignableFrom(p) => item: Item => decodeWKB(item.getBinary(name))
//    }
//  }
//
//  def decodeDate(d: Long): Date = new Date(d)
//
//  def decodeUUID(b: Array[Byte]): UUID = {
//    val bb = ByteBuffer.wrap(b)
//    new UUID(bb.getLong, bb.getLong)
//  }
//
//  def decodeWKB(b: Array[Byte]): Point = WKBUtils.read(b).asInstanceOf[Point]
//
//
//}