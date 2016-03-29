/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.util.concurrent.Future

import com.amazonaws.services.dynamodbv2.document.{Item, ItemCollection, QueryOutcome}
import com.amazonaws.services.dynamodbv2.model.{QueryRequest, QueryResult}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.dynamo.core.DynamoGeoQuery
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class DynamoDBFeatureStore(ent: ContentEntry)
  extends ContentFeatureStore(ent, Query.ALL) with DynamoGeoQuery {

  private lazy val contentState: DynamoDBContentState = {
    entry.getState(getTransaction).asInstanceOf[DynamoDBContentState]
  }

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    getReaderInternal(query, contentState.sft)
  }

  override def getWriterInternal(q: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    if ((flags | WRITER_ADD) == WRITER_ADD) {
      new DynamoDBAppendingFeatureWriter(contentState.sft, contentState.table)
    }
    else {
      new DynamoDBUpdatingFeatureWriter(contentState.sft, contentState.table)
    }
  }

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = WHOLE_WORLD

  override def getCountOfAll: Int = checkLongToInt(contentState.getCountOfAll)

  override def getCountInternal(query: Query): Int = getCountInternal(query, contentState.sft)

  override def executeGeoTimeCountQuery(query: Query, plans: Iterator[HashAndRangeQueryPlan]): Long = {
    if (plans.size > 10) {
      -1L
    } else {
      plans.map { case HashAndRangeQueryPlan(r, l, u, c) =>
        val q = contentState.geoTimeCountQuery(r, l, u)
        val res = contentState.table.query(q)
        res.getTotalCount
      }.sum.toLong
    }
  }

  def executeGeoTimeQuery(query: Query, plans: Iterator[HashAndRangeQueryPlan]): Iterator[SimpleFeature] = {
    contentState.builderPool.withResource { builder =>
      val results = plans.map { case HashAndRangeQueryPlan(r, l, u, c) =>
        val qs = contentState.geoTimeQuery(r, l, u)
        val q: QueryRequest = new QueryRequest().withTableName("")
        val res = contentState.dynamodbClient.queryAsync(q)
        (c, res)
      }
//      results.flatMap { case (contains, fut) =>
//        postProcess(query,  builder, contains, fut)
//      }
      results.flatMap { case (contains, fut) =>
        postProcess(query, builder, contains, fut.get())
      }
    }


  }

  def postProcess(q: Query, builder: SimpleFeatureBuilder, contains: Boolean, fut: Future[QueryResult]): Iterator[SimpleFeature] = {
    applyFilter(q, contains, fut..get().getItems.map(i => convertItemToSF(i, builder)))
  }

  override def getFeaturesInternal: Iterator[SimpleFeature] = {
    contentState.builderPool.withResource { builder =>
      contentState.table.scan(contentState.ALL_QUERY).iterator().map {
        i => convertItemToSF(i, builder)
      }
    }
  }

  private def convertItemToSF(i: Item, builder: SimpleFeatureBuilder): SimpleFeature = {
    val fid = i.getString(DynamoDBDataStore.fID)
    val itemAttrs = i.asMap()
    itemAttrs.remove(DynamoDBDataStore.fID)
    val attrs = itemAttrs.valuesIterator.toArray
    builder.reset()
    builder.buildFeature(fid, attrs)
  }

  def planQuery(query: Query): Iterator[HashAndRangeQueryPlan] = {
    planQuery(query, contentState.sft)
  }

}

