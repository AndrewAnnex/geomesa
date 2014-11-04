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

import java.awt.image.BufferedImage
import java.nio.ByteBuffer

import breeze.linalg.DenseMatrix
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.geometry.Envelope2D
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.raster.utils.RasterUtils.doubleRasterToGridCoverage2d
import org.opengis.filter.identity.FeatureId
import org.opengis.geometry.{BoundingBox, Envelope}

class GeomesaRasterFeature(id: FeatureId) extends RasterTrait with Serializable {

  private var chunkData = None: Option[ByteBuffer]
  private var envelope = None: Option[Envelope2D]
  private var band = None: Option[String]
  private var resolution = None: Option[Double]
  private var units = None: Option[String]
  private var cachedGC = None: Option[GridCoverage2D]

//  private var X = None: Option[Int]
//  private var Y = None: Option[Int]
//
//  private var w = None: Option[Int]
//  private var h = None: Option[Int]

  def setBand(b: String) = band = Some(b)
  def setResolution(d: Double) = resolution = Some(d)
  def setUnits(u: String) = units = Some(u)

  def setEncodedChunkData(chunk: Array[Array[Double]]) = chunkData = Some(flattenRasterToNIO(chunk))

  def setEncodedChunkData(chunk: BufferedImage) = {
    val data = chunk.getData
    val x = data.getWidth
    val y = data.getHeight
    chunkData = Some(flattenRasterToNIO(x, y, data.getPixels(0, 0, x, y, null)))
  }

  def setEnvelope(env: Envelope2D) = envelope = Some(env)
  def setEnvelope(env: Envelope) = envelope = Some(new Envelope2D(env))

  def getEnvelope: Envelope = envelope.asInstanceOf[Envelope]
  def getEnvelope2D = envelope

  def getBounds: BoundingBox = getEnvelope2D match {
    case Some(e) =>
      new ReferencedEnvelope(e, e.getCoordinateReferenceSystem)
    case _ => throw UninitializedFieldError("Error, no envelope")
  }

  def getEncodedChunkData = chunkData

  def getDecodedChunkData: Array[Array[Double]] = getEncodedChunkData match {
    case Some(ecd) => upufNIOTo2DArray(ecd)
    case _ => throw UninitializedFieldError("Error, no encoded chunk data available ")
  }

  def getDecodedChunkDataAsDM: DenseMatrix[Double] = getEncodedChunkData match {
    case Some(ecd) => upufNIOToDMatrix(ecd)
    case _ => throw UninitializedFieldError("Error, no encoded chunk data available ")
  }

  // Gets the GC2d if Some(gc) exists, else call the method that optionally sets it and return Some(gc) or None
  def getGridCoverage2d = cachedGC match {
    case Some(gc) => gc
    case _ => setSomeGridCoverage
  }

  private def setSomeGridCoverage = (chunkData, envelope) match {
    case (Some(c), Some(e)) =>
      //This can be something other than a float array
      cachedGC = Some(doubleRasterToGridCoverage2d(getID, getDecodedChunkData, getEnvelope))
      cachedGC
    case _ => None
  }

  def getIdentifier = id
  def getID = id.getID
  def getBand = band
  def getResolution = resolution
  def getUnits = units

}
