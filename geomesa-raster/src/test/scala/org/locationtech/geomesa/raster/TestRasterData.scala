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

package org.locationtech.geomesa.raster

import breeze.linalg.DenseMatrix
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.utils.geohash.BoundingBox

object TestRasterData extends Logging {
  // we choose to use matrix indexing, ie M x N where M is the number of rows and N is the number of Columns

  val oneByThreeTestArray = Array(Array[Double](1.0, 2.0, 3.0)) // one row by three cols, 1x3

  val oneByThreeFlattenedTestArray = oneByThreeTestArray.flatten

  val oneByThreeTestMatrix = DenseMatrix.create(1, 3, Array[Double](1.0, 2.0, 3.0))

  val threeByOneTestArray = Array(Array[Double](1.0), Array[Double](2.0), Array[Double](3.0)) // three rows of one col, 3x1

  val threeByOneFlattenedTestArray = threeByOneTestArray.flatten

  val threeByOneTestMatrix = DenseMatrix.create(3, 1, Array[Double](1.0, 2.0, 3.0))

  val threeByThreeTestIdentMatrix = DenseMatrix.eye[Double](3)

  val threeByThreeTestIdentArray = Array(Array[Double](1.0, 0.0, 0.0), Array[Double](0.0, 1.0, 0.0), Array[Double](0.0, 0.0, 1.0))

  val threeByThreeFlattenedTestIdentArray = threeByThreeTestIdentArray.flatten

  val fourByFourTestIdentMatrix = DenseMatrix.eye[Double](4)

  val fourByFourTestIdentArray = Array(Array(1.0, 0.0, 0.0, 0.0), Array(0.0, 1.0, 0.0, 0.0),
                                      Array(0.0, 0.0, 1.0, 0.0), Array(0.0, 0.0, 0.0, 1.0))

  val fourByFourFlattenedTestIdentArray = fourByFourTestIdentArray.flatten

  val chunk128by128TestArray = Array.fill[Double](128, 128)(1.28)

  val chunk128by128TestMatrix = DenseMatrix.fill(128, 128)(1.28)

  val flattenedChunk128by128TestArray = chunk128by128TestArray.flatten

  val chunk256by256TestArray = Array.fill[Double](256, 256)(2.56)

  val chunk256by256TestMatrix = DenseMatrix.fill(256, 256)(2.56)

  val flattenedChunk256by256TestArray = chunk256by256TestArray.flatten

  val chunk512by512TestArray = Array.fill[Double](512, 512)(5.12)

  val chunk512by512TestMatrix = DenseMatrix.fill(512, 512)(5.12)

  val flattenedChunk512by512TestArray = chunk512by512TestArray.flatten

  // test other types
  val byteChunk = Array.fill[Byte](128, 128)(1)
  val shortChunk = Array.fill[Short](128, 128)(1)
  val intChunk = Array.fill[Int](128, 128)(1)
  val floatChunk = Array.fill[Float](128, 128)(1)
  val longChunk = Array.fill[Long](128, 128)(1)
  val doubleChunk = chunk128by128TestArray

  // test geometries
  val crs = DefaultGeographicCRS.WGS84
  val quadrantIbbox = new ReferencedEnvelope(0.0, 180.0, 0.0, 90.0, crs)
  val quadrantIIbbox = BoundingBox.apply(-180.0, 0.0, 0.0, 90.0)
  val quadrantIIIbbox = BoundingBox.apply(-180.0, 0.0, -90.0, 0.0)
  val quadrantIVbbox = BoundingBox.apply(0.0, 180.0, -90.0, 0.0)

  val quadrantIEnvelope = quadrantIbbox
  val quadrantIIEnvelope = quadrantIIbbox.envelope
  val quadrantIIIEnvelope = quadrantIIIbbox.envelope
  val quadrantIVEnvelope = quadrantIVbbox.envelope


}
