/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.locationtech.geomesa.raster.data

import com.google.common.collect.{ImmutableMap => IMap, ImmutableSetMultimap}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => ARange}
import org.apache.hadoop.io.Text
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.process.knn.TouchingGeoHashes
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.iterators.RasterFilteringIterator
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeohashUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.Try

object AccumuloRasterQueryPlanner extends Logging with IndexFilterHelpers {

  def modifyHashRange(hash: String, expectedLen: Int, res: String): ARange = expectedLen match {
    // JNH: Think about 0-bit GH some more.
    case 0                                     => new ARange(new Text(s"~$res~"))
    case lucky if expectedLen == hash.length   => new ARange(new Text(s"~$res~$hash"))
    case shorten if expectedLen < hash.length  => new ARange(new Text(s"~$res~${hash.substring(0, expectedLen)}"))
    case lengthen if expectedLen > hash.length => new ARange(new Text(s"~$res~$hash"), new Text(s"~$res~$hash~"))
  }

  // The two geometries must at least have some intersection that is two-dimensional
  def improvedOverlaps(a: Geometry, b: Geometry): Boolean = a.relate(b, "2********")

  // Given a Query, determine the closest resolution that has coverage over the bounds
  def getAcceptableResolution(rq: RasterQuery, resAndBoundsMap: IMap[Double, BoundingBox]): Option[Double] = {
    val availableResolutions = resAndBoundsMap.keySet().toList.sorted
    val preferredRes: Double = selectResolution(rq.resolution, availableResolutions)
    getCoarserBounds(rq.bbox, preferredRes, resAndBoundsMap)
  }

  def getCoarserBounds(queryBounds: BoundingBox, res: Double, resToBounds: IMap[Double, BoundingBox]): Option[Double] =
    resToBounds.keys.toArray.filter(_ >= res).sorted.find(c => improvedOverlaps(queryBounds.geom, resToBounds(c).geom))

  def getQueryPlan(rq: RasterQuery, resAndGeoHashMap: ImmutableSetMultimap[Double, Int],
                    resAndBoundsMap: IMap[Double, BoundingBox]): Option[QueryPlan] = {
    // Step 1. Pick resolution and Make sure the query extent is contained in the extent at that resolution
    val selectedRes: Double = getAcceptableResolution(rq, resAndBoundsMap).getOrElse(defaultResolution)
    val res = lexiEncodeDoubleToString(selectedRes)

    // Step 2. Pick GeoHashLength
    val GeoHashLenList = resAndGeoHashMap.get(selectedRes).toList
    val expectedGeoHashLen = if (GeoHashLenList.isEmpty) {
      0
    } else {
      GeoHashLenList.max
    }

    // Step 3. Given an expected Length and the query, pad up or down the CAGH
    val closestAcceptableGeoHash = GeohashUtils.getClosestAcceptableGeoHash(rq.bbox)

    val hashes: List[String] = closestAcceptableGeoHash match {
      case Some(gh) =>
        val preliminaryHashes = List(gh.hash)
        if (rq.bbox.equals(gh.bbox) || gh.bbox.covers(rq.bbox)) {
          preliminaryHashes
        } else {
          val touching = TouchingGeoHashes.touching(gh).map(_.hash)
          (preliminaryHashes ++ touching).distinct
        }
      case _ => Try {BoundingBox.getGeoHashesFromBoundingBox(rq.bbox) } getOrElse List.empty[String]
    }

    // Step 4. Arrive at final ranges
    val r = hashes.map { gh => modifyHashRange(gh, expectedGeoHashLen, res) }.distinct

    if (r.isEmpty) {
      logger.warn(s"AccumuloRasterQueryPlanner: Query was invalid given RasterQuery: $rq")
      None
    } else {
      // of the Ranges enumerated, get the merge of the overlapping Ranges
      val rows = ARange.mergeOverlapping(r)
      logger.debug(s"AccumuloRasterQueryPlanner: Decided to Scan at res: $selectedRes, at rows: $rows, for BBox: ${rq.bbox}")
      // setup the RasterFilteringIterator
      val cfg = new IteratorSetting(90, "raster-filtering-iterator", classOf[RasterFilteringIterator])
      configureRasterFilter(cfg, AccumuloRasterQueryPlanner.constructRasterFilter(rq.bbox.geom, indexSFT))
      configureRasterMetadataFeatureType(cfg, indexSFT)

      // TODO: WCS: setup a CFPlanner to match against a list of strings
      // ticket is GEOMESA-559
      Some(QueryPlan(Seq(cfg), rows, Seq()))
    }
  }

  def selectResolution(suggestedResolution: Double, availableResolutions: List[Double]): Double = {
    logger.debug(s"RasterQueryPlanner: trying to get resolution $suggestedResolution " +
      s"from available Resolutions: ${availableResolutions.sorted}")
    val ret = availableResolutions match {
      case empty if availableResolutions.isEmpty   => 1.0
      case one if availableResolutions.length == 1 => availableResolutions.head
      case _                                       =>
            val lowerResolutions = availableResolutions.filter(_ <= suggestedResolution)
            logger.debug(s"RasterQueryPlanner: Picking a resolution from: $lowerResolutions")
            lowerResolutions match {
              case Nil => availableResolutions.min
              case _ => lowerResolutions.max
            }
    }
    logger.debug(s"RasterQueryPlanner: Decided to use resolution: $ret")
    ret
  }

  def configureRasterFilter(cfg: IteratorSetting, filter: Filter) = {
    cfg.addOption(DEFAULT_FILTER_PROPERTY_NAME, ECQL.toCQL(filter))
  }

  def configureRasterMetadataFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  val ff = CommonFactoryFinder.getFilterFactory2

  def constructRasterFilter(geom: Geometry, featureType: SimpleFeatureType): Filter = {
    val property = ff.property(featureType.getGeometryDescriptor.getLocalName)
    val bounds = ff.literal(geom)
    ff.and(ff.intersects(property, bounds), ff.not(ff.touches(property, bounds)))
  }

}

case class ResolutionPlanner(ires: Double) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(lexiEncodeDoubleToString(ires)))
}

case class BandPlanner(band: String) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(band))
}
