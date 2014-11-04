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

package org.locationtech.geomesa.raster.utils

import org.geotools.coverage.grid.GridCoverage2D
import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.TestRasterData._
import org.locationtech.geomesa.raster.utils.RasterUtils._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterUtilsTest extends Specification {
  sequential

  "RasterUtils" should {

    "Construct a valid WritableRaster from a flattened 1(h) by 3(w) Double test Raster" in {
      val testWR = doubleFlatRasterToWritableRaster(3, 1, oneByThreeFlattenedTestArray)

      testWR.getWidth must beEqualTo(3)
      testWR.getHeight must beEqualTo(1)
      //testWR.getClass mustEqual classOf[WritableRaster]
    }

    "Construct a valid WritableRaster from a flattened 3(h) by 1(w) Double test Raster" in {
      val testWR = doubleFlatRasterToWritableRaster(1, 3, threeByOneFlattenedTestArray)

      testWR.getWidth must beEqualTo(1)
      testWR.getHeight must beEqualTo(3)
    }

    "Construct a valid WritableRaster from a flattened 3 by 3 Double test Raster" in {
      val testWR = doubleFlatRasterToWritableRaster(3, 3, threeByThreeFlattenedTestIdentArray)

      testWR.getWidth must beEqualTo(3)
      testWR.getHeight must beEqualTo(3)
    }

    "Construct a valid WritableRaster from a flattened 4 by 4 Double test Raster" in {
      val testWR = doubleFlatRasterToWritableRaster(4, 4, fourByFourFlattenedTestIdentArray)

      testWR.getWidth must beEqualTo(4)
      testWR.getHeight must beEqualTo(4)
    }

    "Construct a valid WritableRaster from a flattened 128 by 128 Double test Raster" in {
      val testWR = doubleFlatRasterToWritableRaster(128, 128, flattenedChunk128by128TestArray)

      testWR.getWidth must beEqualTo(128)
      testWR.getHeight must beEqualTo(128)
    }

    "Construct a valid WritableRaster from a flattened 256 by 256 Double test Raster" in {
      val testWR = doubleFlatRasterToWritableRaster(256, 256, flattenedChunk256by256TestArray)

      testWR.getWidth must beEqualTo(256)
      testWR.getHeight must beEqualTo(256)
    }

    "Construct a valid WritableRaster from a flattened 512 by 512 Double test Raster" in {
      val testWR = doubleFlatRasterToWritableRaster(512, 512, flattenedChunk512by512TestArray)

      testWR.getWidth must beEqualTo(512)
      testWR.getHeight must beEqualTo(512)
    }

    "Construct a valid WritableRaster from a 1(h) by 3(w) Double test Raster" in {
      val testWR = doubleRasterToWritableRaster(oneByThreeTestArray)

      testWR.getWidth must beEqualTo(3)
      testWR.getHeight must beEqualTo(1)
    }

    "Construct a valid WritableRaster from a 3(h) by 1(w) Double test Raster" in {
      val testWR = doubleRasterToWritableRaster(threeByOneTestArray)

      testWR.getWidth must beEqualTo(1)
      testWR.getHeight must beEqualTo(3)
    }

    "Construct a valid WritableRaster from a 3 by 3 Double test Raster" in {
      val testWR = doubleRasterToWritableRaster(threeByThreeTestIdentArray)

      testWR.getWidth must beEqualTo(3)
      testWR.getHeight must beEqualTo(3)
    }

    "Construct a valid WritableRaster from a 4 by 4 Double test Raster" in {
      val testWR = doubleRasterToWritableRaster(fourByFourTestIdentArray)

      testWR.getWidth must beEqualTo(4)
      testWR.getHeight must beEqualTo(4)
    }

    "Construct a valid WritableRaster from a 128 by 128 Double test Raster" in {
      val testWR = doubleRasterToWritableRaster(chunk128by128TestArray)

      testWR.getWidth must beEqualTo(128)
      testWR.getHeight must beEqualTo(128)
    }

    "Construct a valid WritableRaster from a 256 by 256 Double test Raster" in {
      val testWR = doubleRasterToWritableRaster(chunk256by256TestArray)

      testWR.getWidth must beEqualTo(256)
      testWR.getHeight must beEqualTo(256)
    }

    "Construct a valid WritableRaster from a 512 by 512 Double test Raster" in {
      val testWR = doubleRasterToWritableRaster(chunk512by512TestArray)

      testWR.getWidth must beEqualTo(512)
      testWR.getHeight must beEqualTo(512)
    }

    "Construct a valid GridCoverage2d in Quadrant I from a 1(h) by 3(w) Double test Raster" in {
      val testGrid = doubleRasterToGridCoverage2d("testGrid", oneByThreeTestArray, quadrantIEnvelope)

      testGrid.getRenderedImage.getHeight must beEqualTo(1)
      testGrid.getRenderedImage.getWidth must beEqualTo(3)
      testGrid.getClass mustEqual classOf[GridCoverage2D]
    }

    "Construct a valid GridCoverage2d in Quadrant I from a 3(h) by 1(w) Double test Raster" in {
      val testGrid = doubleRasterToGridCoverage2d("testGrid", threeByOneTestArray, quadrantIEnvelope)

      testGrid.getRenderedImage.getHeight must beEqualTo(3)
      testGrid.getRenderedImage.getWidth must beEqualTo(1)
      testGrid.getClass mustEqual classOf[GridCoverage2D]
    }

    "Construct a valid GridCoverage2d in Quadrant I from a 3 by 3 Identity Double test Raster" in {
      val testGrid = doubleRasterToGridCoverage2d("testGrid", threeByThreeTestIdentArray, quadrantIEnvelope)

      testGrid.getRenderedImage.getHeight must beEqualTo(3)
      testGrid.getRenderedImage.getWidth must beEqualTo(3)
      testGrid.getClass mustEqual classOf[GridCoverage2D]
    }

    "Construct a valid GridCoverage2d in Quadrant I from a 4 by 4 Identity Double test Raster" in {
      val testGrid = doubleRasterToGridCoverage2d("testGrid", fourByFourTestIdentArray, quadrantIEnvelope)

      testGrid.getRenderedImage.getHeight must beEqualTo(4)
      testGrid.getRenderedImage.getWidth must beEqualTo(4)
      testGrid.getClass mustEqual classOf[GridCoverage2D]
    }

    "Construct a valid GridCoverage2d in Quadrant I from a 128 by 128 Double test Raster" in {
      val testGrid = doubleRasterToGridCoverage2d("testGrid", chunk128by128TestArray, quadrantIEnvelope)

      testGrid.getRenderedImage.getHeight must beEqualTo(128)
      testGrid.getRenderedImage.getWidth must beEqualTo(128)
      testGrid.getClass mustEqual classOf[GridCoverage2D]
    }

    "Construct a valid GridCoverage2d in Quadrant I from a 256 by 256 Double test Raster" in {
      val testGrid = doubleRasterToGridCoverage2d("testGrid", chunk256by256TestArray, quadrantIEnvelope)

      testGrid.getRenderedImage.getHeight must beEqualTo(256)
      testGrid.getRenderedImage.getWidth must beEqualTo(256)
      testGrid.getClass mustEqual classOf[GridCoverage2D]
    }

    "Construct a valid GridCoverage2d in Quadrant I from a 512 by 512 Double test Raster" in {
      val testGrid = doubleRasterToGridCoverage2d("testGrid", chunk512by512TestArray, quadrantIEnvelope)

      testGrid.getRenderedImage.getHeight must beEqualTo(512)
      testGrid.getRenderedImage.getWidth must beEqualTo(512)
      testGrid.getClass mustEqual classOf[GridCoverage2D]
    }

  }
}
