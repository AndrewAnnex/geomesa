/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.data

import java.awt.image.BufferedImage

import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.RasterTestsUtils._
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryAndMosaicTest extends Specification {
  sequential

  var testIteration = 0

  def getNewIteration() = {
    testIteration += 1
    s"testQAMT_Table_$testIteration"
  }

  def compareBufferedImages(act: BufferedImage, exp: BufferedImage): Boolean = {
    //compare basic info
    if (act.getWidth != exp.getWidth) false
    else if (act.getHeight != exp.getHeight) false
    else {
      val actWR = act.getRaster
      val expWR = exp.getRaster
      val actElements = for(i <- 0 until act.getWidth; j <- 0 until act.getHeight) yield actWR.getSample(i, j, 0)
      val expElements = for(i <- 0 until act.getWidth; j <- 0 until act.getHeight) yield expWR.getSample(i, j, 0)
      actElements.sameElements(expElements)
    }
  }

  "A RasterStore" should {
    "Return the same tile we store" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)
      val testBBox = BoundingBox(-77.1152343750, -77.104248046875, 43.001220703125, 43.0122070313125)

      //populate store
      val testRaster = generateRaster(testBBox, testRasterIntVSplit)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(-77.1152343750, -77.104248046875, 43.001220703125, 43.0122070313125)

      //view results
      rasterStore must beAnInstanceOf[RasterStore]
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, count) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, testBBox)
      count mustEqual 1
      mosaic must beAnInstanceOf[BufferedImage]
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

    "Return the same tile we store along with other tiles pt. 1 with not full precision query" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)
      val bboxOfInterest =  BoundingBox(-77.1152343750, -77.104248046875, 43.001220703125, 43.0122070313125)
      val bboxNotWanted1  = BoundingBox(-77.1152343750, -77.104248046875, 42.9902343750, 43.001220703125)
      val bboxNotWanted2  = BoundingBox(-77.1152343750, -77.104248046875, 43.01220703125, 43.023193359375)

      //populate store
      val extraRaster = generateRaster(bboxNotWanted1, redHerring, "3")
      rasterStore.putRaster(extraRaster)
      val extraRaster2 = generateRaster(bboxNotWanted2, redHerring, "2")
      rasterStore.putRaster(extraRaster2)
      val testRaster = generateRaster(bboxOfInterest, testRasterIntVSplit, "1")
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)

      //view results
      rasterStore must beAnInstanceOf[RasterStore]
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, count) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, bboxOfInterest)
      count mustEqual 3
      mosaic must beAnInstanceOf[BufferedImage]
      // first line is 42
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }.pendingUntilFixed("Fixed failure case one")

    "Return the same tile we store along with other tiles pt. 2 with not full precision query" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)
      val bboxOfInterest =  BoundingBox(-77.1152343750, -77.104248046875, 43.001220703125, 43.0122070313125)
      val bboxNotWanted1  = BoundingBox(-77.1152343750, -77.104248046875, 42.9902343750, 43.001220703125)
      val bboxNotWanted2  = BoundingBox(-77.1152343750, -77.104248046875, 43.01220703125, 43.023193359375)

      //populate store
      val extraRaster = generateRaster(bboxNotWanted1, redHerring, "1")
      rasterStore.putRaster(extraRaster)
      val extraRaster2 = generateRaster(bboxNotWanted2, redHerring, "3")
      rasterStore.putRaster(extraRaster2)
      val testRaster = generateRaster(bboxOfInterest, testRasterIntVSplit, "2")
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)

      //view results
      rasterStore must beAnInstanceOf[RasterStore]
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, count) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, bboxOfInterest)
      count mustEqual 3
      mosaic must beAnInstanceOf[BufferedImage]
      //first line is 42.
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }.pendingUntilFixed("Fixed failure case two")

  }


}
