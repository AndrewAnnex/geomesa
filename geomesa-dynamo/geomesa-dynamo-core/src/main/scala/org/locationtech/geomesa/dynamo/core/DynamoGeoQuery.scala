package org.locationtech.geomesa.dynamo.core

import org.geotools.data.Query
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.GenTraversable

trait DynamoGeoQuery {

  case class HashAndRangeQueryPlan(row: Int, lz3: Long, uz3: Long, contained: Boolean)

  protected val WHOLE_WORLD = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)

  def getAllFeatures: Iterator[SimpleFeature]

  def getAllFeatures(filter: Seq[Filter]): Iterator[SimpleFeature] = {
    getAllFeatures.filter(f => filter.forall(_.evaluate(f)))
  }


  // Query Specific defs
  def planQuery(query: Query): GenTraversable[HashAndRangeQueryPlan]

  def executeGeoTimeCountQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): Long

  def getCountOfAllDynamo: Int

  def getCountInternalDynamo(query: Query): Int = {
    if(query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
      getCountOfAllDynamo
    } else {
      val plans = planQuery(query)
      executeGeoTimeCountQuery(query, plans).toInt
    }
  }




}
