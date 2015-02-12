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

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => ARange}
import org.apache.hadoop.io.Text
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.process.knn.TouchingGeoHashes
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.index.RasterIndexSchema
import org.locationtech.geomesa.raster.iterators.RasterFilteringIterator
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeohashUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

// TODO: Constructor needs info to create Row Formatter
// right now the schema is not used
// TODO: Consider adding resolutions + extent info  https://geomesa.atlassian.net/browse/GEOMESA-645
case class AccumuloRasterQueryPlanner(schema: RasterIndexSchema) extends Logging with IndexFilterHelpers {

  def modifyHashRange(hash: String, expectedLen: Int, res: String): ARange = expectedLen match {
    case 0                                     => new ARange(new Text(s"~$res~"))
    case lucky if expectedLen == hash.length   => new ARange(new Text(s"~$res~$hash"))
    case shorten if expectedLen < hash.length  => new ARange(new Text(s"~$res~${hash.substring(0, expectedLen)}"))
    case lengthen if expectedLen > hash.length => new ARange(new Text(s"~$res~$hash"), new Text(s"~$res~$hash~"))
  }

  def getQueryPlan(rq: RasterQuery, resAndGeoHashMap: Map[Double, Int]): QueryPlan = {
    val availableResolutions = resAndGeoHashMap.keys.toList.distinct.sorted

    // Step 1. Pick resolution
    val selectedRes: Double = getResolution(rq.resolution, availableResolutions)
    val res = lexiEncodeDoubleToString(selectedRes)

    // Step 2. Pick GeoHashLength, this will need to pick the smallest val if a mulitmap
    val expectedGeoHashLen = resAndGeoHashMap.getOrElse(selectedRes, 0)

    // Step 3. Given an expected Length and the query pad up or down the CAGH
    val closestAcceptableGeoHash = GeohashUtils.getClosestAcceptableGeoHash(rq.bbox)
    val bboxHashes = BoundingBox.getGeoHashesFromBoundingBox(rq.bbox)

    val hashes: List[String] = closestAcceptableGeoHash match {
      case Some(gh) =>
        if (rq.bbox.equals(gh.bbox)) {
          List(gh.hash)
        } else {
          val preliminaryhashes = bboxHashes :+ gh.hash
          if(gh.bbox.covers(rq.bbox)) {
            preliminaryhashes.distinct
          } else {
            val touching = TouchingGeoHashes.touching(gh).map(_.hash)
            (preliminaryhashes ++ touching).distinct
          }
        }
      case        _ => bboxHashes.toList
    }

    logger.debug(s"RasterQueryPlanner: BBox: ${rq.bbox} has geohashes: $hashes, and has encoded Resolution: $res")

    val r = hashes.map{ gh => modifyHashRange(gh, expectedGeoHashLen, res) }.distinct

    // of the Ranges enumerated, get the merge of the overlapping Ranges
    val rows = ARange.mergeOverlapping(r)
    println(s"Buckshot: Scanning with ranges: $rows")

    // setup the RasterFilteringIterator
    val cfg = new IteratorSetting(90, "raster-filtering-iterator", classOf[RasterFilteringIterator])
    configureRasterFilter(cfg, constructFilter(getReferencedEnvelope(rq.bbox), indexSFT))
    configureRasterMetadataFeatureType(cfg, indexSFT)

    // TODO: WCS: setup a CFPlanner to match against a list of strings
    // ticket is GEOMESA-559
    QueryPlan(Seq(cfg), rows, Seq())
  }

  def getLexicodedResolution(suggestedResolution: Double, availableResolutions: List[Double]): String =
    lexiEncodeDoubleToString(getResolution(suggestedResolution, availableResolutions))

  def getResolution(suggestedResolution: Double, availableResolutions: List[Double]): Double = {
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

  def constructFilter(ref: ReferencedEnvelope, featureType: SimpleFeatureType): Filter = {
    val ff = CommonFactoryFinder.getFilterFactory2
    val b = ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), ref)
    b.asInstanceOf[Filter]
  }

  def configureRasterFilter(cfg: IteratorSetting, filter: Filter) = {
    cfg.addOption(DEFAULT_FILTER_PROPERTY_NAME, ECQL.toCQL(filter))
  }

  def configureRasterMetadataFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) = {
    val encodedSimpleFeatureType = SimpleFeatureTypes.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  def getReferencedEnvelope(bbox: BoundingBox): ReferencedEnvelope = {
    val env = bbox.envelope
    new ReferencedEnvelope(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY, DefaultGeographicCRS.WGS84)
  }

}

case class ResolutionPlanner(ires: Double) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(lexiEncodeDoubleToString(ires)))
}

case class BandPlanner(band: String) extends KeyPlanner {
  def getKeyPlan(filter:KeyPlanningFilter, output: ExplainerOutputType) = KeyListTiered(List(band))
}
