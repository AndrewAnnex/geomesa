/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.util

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.model._
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.Transaction
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource, ContentState}
import org.geotools.feature.NameImpl
import org.joda.time.{Seconds, Weeks, DateTime}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class DynamoDBDataStore(catalog: String, dynamoDB: DynamoDB, catalogPt: ProvisionedThroughput) extends ContentDataStore with SchemaValidation with LazyLogging {
  import DynamoDBDataStore._

  private val CATALOG_TABLE = catalog
  private val catalogTable: Table = getOrCreateCatalogTable(dynamoDB, CATALOG_TABLE, catalogPt.getReadCapacityUnits, catalogPt.getWriteCapacityUnits)

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val sft = Option(entry.getState(Transaction.AUTO_COMMIT).getFeatureType).getOrElse { DynamoDBDataStore.getSchema(entry, catalogTable) }
    val table = dynamoDB.getTable(makeTableName(catalog, sft.getTypeName))
    new DynamoDBFeatureStore(entry, sft, table)
  }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    validatingCreateSchema(featureType, createSchemaImpl)
  }

  private def createSchemaImpl(featureType: SimpleFeatureType): Unit = {
    import java.{lang => jl}

    val attrDefs =
      featureType.getAttributeDescriptors.map { attr =>
        attr.getType.getBinding match {
          case c if c.equals(classOf[jl.Boolean]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType("BOOL")

          case c if c.equals(classOf[jl.Integer]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if c.equals(classOf[jl.Double]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if c.equals(classOf[String]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.S)

          case c if c.equals(classOf[java.util.Date]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if classOf[com.vividsolutions.jts.geom.Geometry].isAssignableFrom(c) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.B)
        }
      }

    val name = featureType.getTypeName
    val rcu: Long = featureType.userData[Long](rcuKey).getOrElse(1L)
    val wcu: Long = featureType.userData[Long](wcuKey).getOrElse(1L)

    val tableDesc =
      new CreateTableRequest()
        .withTableName(makeTableName(catalog, name))
        .withKeySchema(featureKeySchema)
        .withAttributeDefinitions(featureAttributeDescriptions ++ attrDefs) //TODO: do we really want to bother with all these other attributes?
        .withProvisionedThroughput(new ProvisionedThroughput(rcu, wcu))

    // create the table
    val res = dynamoDB.createTable(tableDesc)
    res.waitForActive()

    // write the meta-data
    val metaEntry = createDDMMetaDataItem(name, featureType)
    catalogTable.putItem(metaEntry)
  }

  override def createTypeNames(): util.List[Name] = {
    // read types from catalog
    catalogTable.scan().iterator().map { i => new NameImpl(i.getString(catalogKeyHash)) }.toList
  }

  override def createContentState(entry: ContentEntry): ContentState = {
    new DynamoDBContentState(entry, catalogTable)
  }

  override def dispose(): Unit = if (dynamoDB != null) dynamoDB.shutdown()

  private def createDDMMetaDataItem(name: String, featureType: SimpleFeatureType): Item = {
    new Item().withPrimaryKey(catalogKeyHash, name).withString(catalogSftAttributeName, SimpleFeatureTypes.encodeType(featureType))
  }

  def updateProvisionedThroughput(name: String, pt: ProvisionedThroughput): Unit = {
    val tableName = makeTableName(catalog, name)
    logger.info("Attempting to Modify provisioned throughput for {}", tableName)
    try {
      val table = dynamoDB.getTable(tableName)
      table.updateTable(pt)
      table.waitForActive()
      logger.info(s"Updated table: $tableName to have ProvisionedThroughput: ${pt.toString}")
    } catch {
      case NonFatal(e) => logger.error(s"Unable to update table: $tableName", e)
    }
  }

}

object DynamoDBDataStore {
  val rcuKey = "geomesa.dynamodb.rcu"
  val wcuKey = "geomesa.dynamodb.wcu"

  val serId  = "ser"

  val geomesaKeyHash  = "dtgandz2"
  val geomesaKeyRange = "z3andID"

  val featureKeySchema = List(
      new KeySchemaElement().withAttributeName(geomesaKeyHash).withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName(geomesaKeyRange).withKeyType(KeyType.RANGE)
    )

  val featureAttributeDescriptions = List(
    new AttributeDefinition(geomesaKeyHash,  ScalarAttributeType.B),
    new AttributeDefinition(geomesaKeyRange, ScalarAttributeType.B)
  )

  val catalogKeyHash = "feature"
  val catalogSftAttributeName = "sft"

  val catalogKeySchema = List(new KeySchemaElement(catalogKeyHash, KeyType.HASH))
  val catalogAttributeDescriptions =  List(new AttributeDefinition(catalogSftAttributeName, ScalarAttributeType.S))

  def makeTableName(catalog: String, name: String): String = s"${catalog}_${name}_z3"

  def getSchema(entry: ContentEntry, catalogTable: Table): SimpleFeatureType  = {
    val item = catalogTable.getItem("feature", entry.getTypeName)
    SimpleFeatureTypes.createType(entry.getTypeName, item.getString("sft"))
  }

  private def getOrCreateCatalogTable(dynamoDB: DynamoDB, table: String, rcus: Long = 1L, wcus: Long = 1L) = {
    val tables = dynamoDB.listTables().iterator().toList
    val ret = tables
      .find(_.getTableName == table)
      .getOrElse(
        dynamoDB.createTable(
          table,
          catalogKeySchema,
          catalogAttributeDescriptions,
          new ProvisionedThroughput(rcus, wcus)
        )
      )
    ret.waitForActive()
    ret
  }

  def apply(catalog: String, dynamoDB: DynamoDB, rcus: Long, wcus: Long): DynamoDBDataStore = {
    val pt = new ProvisionedThroughput(rcus, wcus)
    new DynamoDBDataStore(catalog, dynamoDB, pt)
  }

}

trait SchemaValidation {

  protected def validatingCreateSchema(featureType: SimpleFeatureType, cs: (SimpleFeatureType) => Unit): Unit = {
    // validate dtg
    featureType.getAttributeDescriptors
      .find { ad => ad.getType.getBinding.isAssignableFrom(classOf[java.util.Date]) }
      .getOrElse(throw new IllegalArgumentException("Could not find a dtg field"))

    // validate geometry
    featureType.getAttributeDescriptors
      .find { ad => ad.getType.getBinding.isAssignableFrom(classOf[Geometry]) }
      .getOrElse(throw new IllegalArgumentException("Could not find a valid point geometry"))

    cs(featureType)
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