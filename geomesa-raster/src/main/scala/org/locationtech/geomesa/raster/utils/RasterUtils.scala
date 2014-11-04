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

import java.awt.Point
import java.awt.image._

import org.geotools.coverage.grid.GridCoverageFactory
import org.locationtech.geomesa.raster.feature.RasterDataEncoding
import org.opengis.geometry.Envelope

object RasterUtils extends RasterDataEncoding {

  final val defaultAWTPoint = new Point(0, 0)

  final val singleBandOffset = Array.fill[Int](1)(0)

  val defaultGridCoverageFactory = new GridCoverageFactory

  // scanLineStride is the number of pixels between a given sample and the next sample in the same column
  def doubleSingleBandedSampleModel(w: Int, h: Int) = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, w, h, 1)

  def doubleFlatRasterToAWTRaster(w: Int, h:Int, raster: Array[Double]): Raster = {
    val dbBuffer = new DataBufferDouble(raster, w*h)
    Raster.createRaster(doubleSingleBandedSampleModel(w, h), dbBuffer, null)
  }

  def doubleFlatRasterToWritableRaster(w: Int, h:Int, raster: Array[Double]): WritableRaster = {
    val dbBuffer = new DataBufferDouble(raster, w*h)
    Raster.createWritableRaster(doubleSingleBandedSampleModel(w, h), dbBuffer, null)
  }

  def doubleRasterToWritableRaster(raster: Array[Array[Double]]): WritableRaster = {
    val (h, w) = getRasterShape(raster)
    doubleFlatRasterToWritableRaster(w, h, raster.flatten)
  }

  def doubleRasterToAWTRaster(raster: Array[Array[Double]]): Raster = {
    val (h, w) = getRasterShape(raster)
    doubleFlatRasterToAWTRaster(w, h, raster.flatten)
  }

  def doubleFlatRasterToGridCoverage2d(name: String,
                                       w: Int, h: Int,
                                       raster: Array[Double],
                                       env: Envelope) = {
    defaultGridCoverageFactory.create(name, doubleFlatRasterToWritableRaster(w, h, raster), env)
  }

  def doubleRasterToGridCoverage2d(name: String, raster: Array[Array[Double]], env: Envelope) = {
    defaultGridCoverageFactory.create(name, doubleRasterToWritableRaster(raster), env)
  }

}
