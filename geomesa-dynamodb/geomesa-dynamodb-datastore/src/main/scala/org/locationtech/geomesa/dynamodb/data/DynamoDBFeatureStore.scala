/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.document.{Item, ItemCollection, QueryOutcome, Table}
import com.amazonaws.services.dynamodbv2.model.Select
import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.Interval
import org.locationtech.geomesa.filter._
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.GenTraversable
import scala.collection.JavaConversions._

class DynamoDBFeatureStore(entry: ContentEntry,
                           sft: SimpleFeatureType,
                           table: Table)
  extends ContentFeatureStore(entry, Query.ALL) {

  case class HashAndRangeQueryPlan(row: Int, lz3: Long, uz3: Long, contained: Boolean)

  val WHOLE_WORLD = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)

  private lazy val contentState: DynamoDBContentState = entry.getState(getTransaction).asInstanceOf[DynamoDBContentState]

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = WHOLE_WORLD

  override def getCountInternal(query: Query): Int = {
    // TODO: getItemCount returns a Long, may need to do something safer
    if(query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
      table.getDescription.getItemCount.toInt
    } else {
      val plans = planQuery(query)
      executeGeoTimeCountQuery(query, plans)
    }
  }

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val iter: Iterator[SimpleFeature] =
      if(query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
        getAllFeatures
      } else {
        val plans    = planQuery(query)
        val features = executeGeoTimeQuery(query, plans).toIterator
        features
      }
    new DelegateSimpleFeatureReader(contentState.sft, new DelegateSimpleFeatureIterator(iter))
  }

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    if((flags | WRITER_ADD) == WRITER_ADD) new DynamoDBAppendingFeatureWriter(contentState.sft, contentState.table)
    else                                   new DynamoDBUpdatingFeatureWriter(contentState.sft, contentState.table)
  }

  def getAllFeatures: Iterator[SimpleFeature] = {
    contentState.table.scan(contentState.ALL_QUERY).iterator().map(i => contentState.serializer.deserialize(i.getBinary(DynamoDBDataStore.serId)))
  }

  def planQuery(query: Query): GenTraversable[HashAndRangeQueryPlan] = {
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    val origBounds = query.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, DefaultGeographicCRS.WGS84).asInstanceOf[Envelope]
    val re = WHOLE_WORLD.intersection(new ReferencedEnvelope(origBounds, DefaultGeographicCRS.WGS84))
    val (lx, ly, ux, uy) = (re.getMinX, re.getMinY, re.getMaxX, re.getMaxY)
    val (dtgFilters, _) = partitionPrimaryTemporals(decomposeAnd(query.getFilter), contentState.sft)
    val interval = FilterHelper.extractInterval(dtgFilters, contentState.sft.getDtgField)

    val startWeeks = DynamoDBPrimaryKey.epochWeeks(interval.getStart).getWeeks
    val endWeeks   = DynamoDBPrimaryKey.epochWeeks(interval.getEnd).getWeeks

    val zRanges = DynamoDBPrimaryKey.SFC2D.toRanges(lx, ly, ux, uy).toList

    val rows = (startWeeks to endWeeks).map { dt => getRowKeys(zRanges, interval, startWeeks, endWeeks, dt)}

    val plans =
      rows.flatMap { case ((s, e), rowRanges) =>
        planQueryForContiguousRowRange(s, e, rowRanges)
      }
    plans
  }

  def getRowKeys(zRanges: Seq[IndexRange], interval: Interval, sew: Int, eew: Int, dt: Int): ((Int, Int), Seq[Int]) = {
    val dtshift = dt << 16
    val seconds: (Int, Int) =
      if (dt != sew && dt != eew) {
        (0, DynamoDBPrimaryKey.ONE_WEEK_IN_SECONDS)
      } else {
        val starts = if (dt == sew) DynamoDBPrimaryKey.secondsInCurrentWeek(interval.getStart) else 0
        val ends   = if (dt == eew) DynamoDBPrimaryKey.secondsInCurrentWeek(interval.getEnd)   else DynamoDBPrimaryKey.ONE_WEEK_IN_SECONDS
        (starts, ends)
      }

    val shiftedRanges = zRanges.flatMap { ir =>
      val (l, u, _) = ir.tuple
      (l to u).map { i => (dtshift + i).toInt }
    }

    (seconds, shiftedRanges)
  }

  def planQueryForContiguousRowRange(s: Int, e: Int, rowRanges: Seq[Int]): Seq[HashAndRangeQueryPlan] = {
    rowRanges.flatMap { r =>
      val DynamoDBPrimaryKey.Key(_, _, _, _, z) = DynamoDBPrimaryKey.unapply(r)
      val (minx, miny, maxx, maxy) = DynamoDBPrimaryKey.SFC2D.bound(z)
      val z3ranges = DynamoDBPrimaryKey.SFC3D.ranges((minx, maxx), (miny, maxy), (s, e)).toList

      z3ranges.map { ir =>
        val (l, u, contains) = ir.tuple
        HashAndRangeQueryPlan(r, l, u, contains)
      }
    }

  }

  def executeGeoTimeQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): GenTraversable[SimpleFeature] = {
    val results = plans.map { case HashAndRangeQueryPlan(r, l, u, c) =>
      val q = contentState.geoTimeQuery(r, l, u)
      val res = contentState.table.query(q)
      (c, res)
    }
    results.flatMap{ case (contains, fut) =>
      postProcessResults(query, contains, fut)
    }
  }

  def executeGeoTimeCountQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): Int = {
    val results = plans.map { case HashAndRangeQueryPlan(r, l, u, c) =>
      val q = contentState.geoTimeQuery(r, l, u).withSelect(Select.COUNT)
      contentState.table.query(q).getTotalCount // TODO: might not be exactly what we want
    }
    results.sum
  }

  def postProcessResults(query: Query, contains: Boolean, fut: ItemCollection[QueryOutcome]): Iterator[SimpleFeature] = {
    val filter = query.getFilter
    fut.toIterator.flatMap { i =>
      val tempSF = convertItemToSF(i)
      if (!contains) {
        if (filter.evaluate(tempSF)) Some(tempSF) else None
      } else {
        Some(tempSF)
      }
    }
    //fut.toIterator.map(convertItemToSF)
  }

  private def convertItemToSF(i: Item): SimpleFeature = {
    contentState.serializer.deserialize(i.getBinary(DynamoDBDataStore.serId))
  }

}

