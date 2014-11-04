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

package org.locationtech.geomesa.raster.feature

import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.TestRasterData._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterTraitTest extends Specification {

  sequential
  val rasterFuncs = new RasterTrait { }

  "RasterTrait" should {

    "flatten a 1 by 3 test Raster into a Array[Byte] that includes dimensionality" in {
      val r = rasterFuncs.flattenRasterIncDim(oneByThreeTestArray)

      r.length must beEqualTo(32)
    }

    "flatten a 3 by 1 test Raster into a Array[Byte] that includes dimensionality" in {
      val r = rasterFuncs.flattenRasterIncDim(threeByOneTestArray)

      r.length must beEqualTo(32)
    }

    "flatten a 3 by 3 test Raster into a Array[Byte] that includes dimensionality" in {
      val r = rasterFuncs.flattenRasterIncDim(threeByThreeTestIdentArray)

      r.length must beEqualTo(80)
    }

    "flatten a 4 by 4 test Raster into a Array[Byte] that includes dimensionality" in {
      val r = rasterFuncs.flattenRasterIncDim(fourByFourTestIdentArray)

      r.length must beEqualTo(136)
    }

    "flatten a 128 by 128 test Raster into a Array[Byte] that includes dimensionality" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk128by128TestArray)

      r.length must beEqualTo(131072+8)
    }

    "flatten a 256 by 256 test Raster into a Array[Byte] that includes dimensionality" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk256by256TestArray)

      r.length must beEqualTo(524288+8)
    }

    "flatten a 512 by 512 test Raster into a Array[Byte] that includes dimensionality" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk512by512TestArray)

      r.length must beEqualTo(2097152+8)
    }

    "unpack and unflatten a 1 by 3 test Raster to an Array[Double]" in {
      val r = rasterFuncs.flattenRasterIncDim(oneByThreeTestArray)
      val (x, y, u) = rasterFuncs.upufArrayIncDim(r)

      x must beEqualTo(1)
      y must beEqualTo(3)

      u.length must beEqualTo(3)
    }

    "unpack and unflatten a 3 by 1 test Raster to an Array[Double]" in {
      val r = rasterFuncs.flattenRasterIncDim(threeByOneTestArray)
      val (x, y, u) = rasterFuncs.upufArrayIncDim(r)

      x must beEqualTo(3)
      y must beEqualTo(1)

      u.length must beEqualTo(3)
    }

    "unpack and unflatten a 3 by 3 test Raster to an Array[Double]" in {
      val r = rasterFuncs.flattenRasterIncDim(threeByThreeTestIdentArray)
      val (x, y, u) = rasterFuncs.upufArrayIncDim(r)

      x must beEqualTo(3)
      y must beEqualTo(3)

      u.length must beEqualTo(9)
    }

    "unpack and unflatten a 4 by 4 test Raster to an Array[Double]" in {
      val r = rasterFuncs.flattenRasterIncDim(fourByFourTestIdentArray)
      val (x, y, u) = rasterFuncs.upufArrayIncDim(r)

      x must beEqualTo(4)
      y must beEqualTo(4)

      u.length must beEqualTo(16)
    }

    "unpack and unflatten a 128 by 128 test Raster to an Array[Double]" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk128by128TestArray)
      val (x, y, u) = rasterFuncs.upufArrayIncDim(r)

      x must beEqualTo(128)
      y must beEqualTo(128)

      u.length must beEqualTo(16384)
    }

    "unpack and unflatten a 256 by 256 test Raster to an Array[Double]" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk256by256TestArray)
      val (x, y, u) = rasterFuncs.upufArrayIncDim(r)

      x must beEqualTo(256)
      y must beEqualTo(256)

      u.length must beEqualTo(65536)
    }

    "unpack and unflatten a 512 by 512 test Raster to an Array[Double]" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk512by512TestArray)
      val (x, y, u) = rasterFuncs.upufArrayIncDim(r)

      x must beEqualTo(512)
      y must beEqualTo(512)

      u.length must beEqualTo(262144)
    }

    "unpack and unflatten a 1 by 3 test Raster to an Array[Array[Double]]" in {
      val r = rasterFuncs.flattenRasterIncDim(oneByThreeTestArray)
      val u = rasterFuncs.upufArrayTo2DArray(r)

      u.length must beEqualTo(1)
      u(0).length must beEqualTo(3)
    }

    "unpack and unflatten a 3 by 1 test Raster to an Array[Array[Double]]" in {
      val r = rasterFuncs.flattenRasterIncDim(threeByOneTestArray)
      val u = rasterFuncs.upufArrayTo2DArray(r)

      u.length must beEqualTo(3)
      u(0).length must beEqualTo(1)
    }

    "unpack and unflatten a 3 by 3 test Raster to an Array[Array[Double]]" in {
      val r = rasterFuncs.flattenRasterIncDim(threeByThreeTestIdentArray)
      val u = rasterFuncs.upufArrayTo2DArray(r)

      u.length must beEqualTo(3)
      u(0).length must beEqualTo(3)
    }

    "unpack and unflatten a 4 by 4 test Raster to an Array[Array[Double]]" in {
      val r = rasterFuncs.flattenRasterIncDim(fourByFourTestIdentArray)
      val u = rasterFuncs.upufArrayTo2DArray(r)

      u.length must beEqualTo(4)
      u(0).length must beEqualTo(4)
    }

    "unpack and unflatten a 128 by 128 test Raster to an Array[Array[Double]]" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk128by128TestArray)
      val u = rasterFuncs.upufArrayTo2DArray(r)

      u.length must beEqualTo(128)
      u(0).length must beEqualTo(128)
    }

    "unpack and unflatten a 256 by 256 test Raster to an Array[Array[Double]]" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk256by256TestArray)
      val u = rasterFuncs.upufArrayTo2DArray(r)

      u.length must beEqualTo(256)
      u(0).length must beEqualTo(256)
    }

    "unpack and unflatten a 512 by 512 test Raster to an Array[Array[Double]]" in {
      val r = rasterFuncs.flattenRasterIncDim(chunk512by512TestArray)
      val u = rasterFuncs.upufArrayTo2DArray(r)

      u.length must beEqualTo(512)
      u(0).length must beEqualTo(512)
    }

    "unpack and unflatten a 1 by 3 test Raster into a Breeze DenseMatrix" in {
      val f = rasterFuncs.flattenRasterIncDim(oneByThreeTestArray)
      val r = rasterFuncs.upufArrayToDMatrix(f)

      r.rows must beEqualTo(1)
      r.cols must beEqualTo(3)

      r must beEqualTo(oneByThreeTestMatrix)
    }

    "unpack and unflatten a 3 by 1 test Raster into a Breeze DenseMatrix" in {
      val f = rasterFuncs.flattenRasterIncDim(threeByOneTestArray)
      val r = rasterFuncs.upufArrayToDMatrix(f)

      r.rows must beEqualTo(3)
      r.cols must beEqualTo(1)

      r must beEqualTo(threeByOneTestMatrix)
    }

    "unpack and unflatten a 3 by 3 test Raster into a Breeze DenseMatrix" in {
      val f = rasterFuncs.flattenRasterIncDim(threeByThreeTestIdentArray)
      val r = rasterFuncs.upufArrayToDMatrix(f)

      r.rows must beEqualTo(3)
      r.cols must beEqualTo(3)

      r must beEqualTo(threeByThreeTestIdentMatrix)
    }

    "unpack and unflatten a 4 by 4 test Raster into a Breeze DenseMatrix" in {
      val f = rasterFuncs.flattenRasterIncDim(fourByFourTestIdentArray)
      val r = rasterFuncs.upufArrayToDMatrix(f)

      r.rows must beEqualTo(4)
      r.cols must beEqualTo(4)

      r must beEqualTo(fourByFourTestIdentMatrix)
    }

    "unpack and unflatten a 128 by 128 test Raster into a Breeze DenseMatrix" in {
      val f = rasterFuncs.flattenRasterIncDim(chunk128by128TestArray)
      val r = rasterFuncs.upufArrayToDMatrix(f)

      r.rows must beEqualTo(128)
      r.cols must beEqualTo(128)

      r must beEqualTo(chunk128by128TestMatrix)
    }

    "unpack and unflatten a 258 by 256 test Raster into a Breeze DenseMatrix" in {
      val f = rasterFuncs.flattenRasterIncDim(chunk256by256TestArray)
      val r = rasterFuncs.upufArrayToDMatrix(f)

      r.rows must beEqualTo(256)
      r.cols must beEqualTo(256)

      r must beEqualTo(chunk256by256TestMatrix)
    }

    "unpack and unflatten a 512 by 512 test Raster into a Breeze DenseMatrix" in {
      val f = rasterFuncs.flattenRasterIncDim(chunk512by512TestArray)
      val r = rasterFuncs.upufArrayToDMatrix(f)

      r.rows must beEqualTo(512)
      r.cols must beEqualTo(512)

      r must beEqualTo(chunk512by512TestMatrix)
    }

  }

}
