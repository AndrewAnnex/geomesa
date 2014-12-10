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

package org.locationtech.geomesa.raster.iterators

import java.awt.image.{WritableRaster, BufferedImage}

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.GridCoverageFactory
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.data.{RasterQuery, RasterStore}
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterFilteringIteratorTest extends Specification with Logging {

  sequential

  var testIteration = 0
  val coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(new Hints())

  def getNewIteration() = {
    testIteration += 1
    s"testTable_$testIteration"
  }

  // stolen from elsewhere
  def getNewImage(width: Int, height: Int, color: Array[Int]): BufferedImage = {
    val image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    var h = 0
    var w = 0
    for (h <- 1 until height) {
      for (w <- 1 until width) {
        wr.setPixel(w, h, color)
      }
    }
    image
  }

  def imageToCoverage(width: Int, height: Int, img: WritableRaster, env: ReferencedEnvelope, cf: GridCoverageFactory) = {
    cf.create("testRaster", img, env)
  }

  def createRasterStore(tableName: String) = {
    val rs = RasterStore("user", "pass", "testInstance", "zk", tableName, "SUSA", "SUSA", true)
    rs
  }

  def generateQuery(minX: Int, maxX:Int, minY: Int, maxY: Int, res: Double = 10.0) = {
    val bb = BoundingBox(new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84))
    new RasterQuery(bb, res, None, None)
  }

  def generateTestRaster(minX: Int, maxX:Int, minY: Int, maxY: Int, w: Int = 256, h: Int = 256, res: Double = 10.0) = {
    val ingestTime = new DateTime()
    val env = new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84)
    val bbox = BoundingBox(env)
    val metadata = DecodedIndex(Raster.getRasterId("testRaster"), bbox.geom, Option(ingestTime.getMillis))
    val image = getNewImage(w, h, Array[Int](255, 255, 255))
    val coverage = imageToCoverage(w, h, image.getRaster(), env, coverageFactory)
    new Raster(coverage.getRenderedImage, metadata, res)
  }


  "RasterFilteringIterator" should {
    "Properly filter in a raster via a query" in {
      val tableName = getNewIteration()
      val rasterStore = createRasterStore(tableName)

      // general setup
      val testRaster = generateTestRaster(0, 10, 0, 10)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 11, 0, 11)

      rasterStore must beAnInstanceOf[RasterStore]
      val theIterator = rasterStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }

    "Properly filter out a raster via a query" in {
      val tableName = getNewIteration()
      val rasterStore = createRasterStore(tableName)

      // general setup
      val testRaster = generateTestRaster(-10, -20, -10, -20)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 10, 0, 10)

      rasterStore must beAnInstanceOf[RasterStore]
      val theIterator = rasterStore.getRasters(query)
      theIterator.isEmpty must beTrue
    }

    "Properly filter out a raster via a query and but maintain a valid raster in the results" in {
      val tableName = getNewIteration()
      val rasterStore = createRasterStore(tableName)

      // general setup
      val testRaster1 = generateTestRaster(-10, -20, -10, -20)
      rasterStore.putRaster(testRaster1)
      val testRaster2 = generateTestRaster(0, 10, 0, 10)
      rasterStore.putRaster(testRaster2)

      //generate query
      val query = generateQuery(0, 10, 0, 10)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return a group of four Rasters" in {
      val tableName = getNewIteration()
      val rasterStore = createRasterStore(tableName)

      // general setup
      val testRaster1 = generateTestRaster(0, 5, 0, 5)
      rasterStore.putRaster(testRaster1)
      val testRaster2 = generateTestRaster(5, 10, 0, 5)
      rasterStore.putRaster(testRaster2)
      val testRaster3 = generateTestRaster(0, 5, 5, 10)
      rasterStore.putRaster(testRaster3)
      val testRaster4 = generateTestRaster(5, 10, 5, 10)
      rasterStore.putRaster(testRaster4)

      //generate query
      val query = generateQuery(0, 10, 0, 10)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(4)
    }

  }

}
