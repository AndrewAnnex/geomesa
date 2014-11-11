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

import java.nio.ByteBuffer

import breeze.linalg.DenseMatrix

import scala.reflect.ClassTag

trait RasterTrait extends RasterDataEncoding with RasterDataDecoding {


}

trait RasterDataEncoding extends RasterCommons {

  def encodeFlatRasterToBB[T: Numeric](x: Int, y: Int, raster: Array[T]): ByteBuffer = {
    val bb = ByteBuffer.allocate((x*y*8)+8)  //todo: see if allocateDirect is better?
    val setter = getBBSetter(bb)
    bb.putInt(x)
    bb.putInt(y)
    var i = 0
    while (i < raster.length) {
      setter(raster(i))
      i += 1
    }
    bb
  }

  /**
   *  Encode a given raster (2D Array) into a given ByteBuffer and include encoded Dims
   *  Note: Non-Scala Style due to speed requirements.
   * @param x number of rows
   * @param y number of columns
   * @param raster Array[ Array[Double] ]
   * @return ByteBuffer
   */
  def encodeRasterToBB[T: Numeric](x: Int, y: Int, raster: Array[Array[T]]): ByteBuffer = {
    val bb = ByteBuffer.allocate((x*y*8)+8)  //todo: see if allocateDirect is better?
    val setter = getBBSetter(bb)
    bb.putInt(x)
    bb.putInt(y)
    var i, j = 0
    while (i < x) {
      while (j < y) {
        setter(raster(i)(j))
        j+=1
      }
      j = 0
      i+= 1
    }
    bb
  }

  /**
   *  Encode a given raster (2D Array) into a flat Array[Byte] and include encoded Dims
   *  Note: Non-Scala Style due to speed requirements.
   * @param x number of rows
   * @param y number of columns
   * @param raster Array[ Array[Double] ]
   * @return Array[Byte]
   */
  def encodeRaster[T: Numeric](x: Int, y: Int, raster: Array[Array[T]]): Array[Byte] = {
    encodeRasterToBB(x, y, raster).array()
  }

  /**
   *  Encode a given raster and return a tuple of the Dims and the flattened encoded raster Array[Byte]
   * @param raster Array[ Array[Double] ]
   * @return
   */
  def flattenRaster[T: Numeric](raster: Array[Array[T]]): (Int, Int, Array[Byte]) = {
    val (x, y) = getRasterShape(raster)
    (x, y, encodeRaster(x, y, raster))
  }

  /**
   *  Encode a given raster and return the flattened encoded raster Array[Byte]
   * @param raster Array[ Array[Double] ]
   * @return Array[Byte]
   */
  def flattenRasterIncDim[T: Numeric](raster: Array[Array[T]]): Array[Byte] = {
    val (x, y) = getRasterShape(raster)
    encodeRaster(x, y, raster)
  }

  /**
   *  Encode a given raster and return a ByteBuffer
   * @param raster Array[ Array[Double] ]
   * @return ByteBuffer
   */
  def flattenRasterToNIO[T: Numeric](raster: Array[Array[T]]): ByteBuffer = {
    val (x, y) = getRasterShape(raster)
    encodeRasterToBB(x, y, raster)
  }

  def flattenRasterToNIO(raster: DenseMatrix[Double]): ByteBuffer = {
    encodeFlatRasterToBB(raster.rows, raster.cols, raster.toDenseVector.toArray)
  }

  def flattenRasterToNIO(x: Int, y: Int, raster: Array[Double]): ByteBuffer = {
    encodeFlatRasterToBB(x, y, raster)
  }

  /**
   * Given a Array[ Array[Numeric] ], figure out the number of rows and columns
   * @param r a raster, an array of arrays, where the inner array represents
   *          a whole row of elements (one value from each column)
   * @return a tuple containing the (x, y) Dims, x is the number of rows, y is the number of cols
   *
   *         must be like matrix indexing for sanity.
   *         a 1x3 array: [[1.0, 1.0, 1.0]] must return (1, 3).
   *         a 3x1 array: [[1.0],[1.0],[1.0]] must return (3, 1).
   *
   */
  def getRasterShape[T: Numeric](r: Array[Array[T]]): (Int, Int) = r match {
    case Array(_*) =>
      val y = r match {
        case is2d if (r.isDefinedAt(0) && r(0).isDefinedAt(0)) => r(0).length
        case _ => 1
      }
      (r.length, y)
    case _ =>
      (0, 0)
  }

}

trait RasterDataDecoding extends RasterCommons {

  /**
   *  Decodes a given NIO ByteBuffer into a flat Array of Doubles
   *  Note: Non-Scala Style due to speed requirements.
   * @param x number of rows
   * @param y number of columns
   * @param bb ByteBuffer of RasterData sans Encoded Dims
   * @return Array[Double]
   */
  def decodeRaster(x: Int, y:Int, bb: ByteBuffer): Array[Double] = {
    val ret = Array.ofDim[Double](x*y)
    var i = 0
    while (i < x*y) {
      ret.update(i, bb.getDouble)
      i += 1
    }
    ret
  }

  /**
   *  Decodes a given NIO ByteBuffer into a 2D Array of Doubles
   *  Note: Non-Scala Style due to speed requirements.
   * @param x number of rows
   * @param y number of columns
   * @param bb ByteBuffer of RasterData sans Encoded Dims
   * @return Array[ Array[Double] ]
   */
  def decodeRasterTo2D(x: Int, y:Int, bb: ByteBuffer): Array[Array[Double]] = {
    val ret = Array.ofDim[Double](x,y)
    var i, j = 0
    while (i < x) {
      while (j < y) {
        ret(i)(j) = bb.getDouble
        j+=1
      }
      j = 0
      i+= 1
    }
    ret
  }

  /**
   *  Unpack and Unflatten a given Array[Byte] to a tuple containing Dims and the raster as a Array[Double]
   * @param arr Array[Byte]
   * @return tuple3
   */
  def upufArrayIncDim(arr: Array[Byte]): (Int, Int, Array[Double]) = {
    upufNIOIncDim(ByteBuffer.wrap(arr))
  }

  /**
   *  Unpack and Unflatten a given ByteBuffer to a tuple containing Dims and the raster as a Array[Double]
   * @param bb ByteBuffer
   * @return tuple3
   */
  def upufNIOIncDim(bb: ByteBuffer): (Int, Int, Array[Double]) = {
    val x = bb.getInt
    val y = bb.getInt
    (x, y, decodeRaster(x, y, bb))
  }

  /**
   * Unpack and Unflatten a given Array[Byte] to a 2D Array[Double]
   * @param arr Array[Byte]
   * @return  Array[ Array[Double] ]
   */
  def upufArrayTo2DArray(arr: Array[Byte]): Array[Array[Double]] = {
    upufNIOTo2DArray(ByteBuffer.wrap(arr))
  }

  /**
   * Unpack and Unflatten a given ByteBuffer to a 2D Array[Double]
   * @param bb ByteBuffer
   * @return Array[ Array[Double] ]
   */
  def upufNIOTo2DArray(bb: ByteBuffer): Array[Array[Double]] = {
    val x = bb.getInt
    val y = bb.getInt
    decodeRasterTo2D(x, y, bb)
  }

  /**
   * Unpack and Unflatten a given Array[Byte] to a Breeze DenseMatrix
   * This is mostly for testing/convenience
   * @param arr Array[Byte]
   * @return
   */
  def upufArrayToDMatrix(arr: Array[Byte]) = {
    upufNIOToDMatrix(ByteBuffer.wrap(arr))
  }

  /**
   * Unpack and Unflatten a given ByteBuffer to a Breeze DenseMatrix
   * This is mostly for testing/convenience
   * @param bb ByteBuffer
   * @return
   */
  def upufNIOToDMatrix(bb: ByteBuffer) = {
    val x = bb.getInt
    val y = bb.getInt
    val r = decodeRaster(x, y, bb)
    DenseMatrix.create(x, y, r)
  }

}

trait RasterCommons {

  def getBBSetter[T](bb: ByteBuffer) = (n: T) => n match {
    case b: Byte   => bb.put(b)
    case s: Short  => bb.putShort(s)
    case i: Int    => bb.putInt(i)
    case l: Long   => bb.putLong(l)
    case f: Float  => bb.putFloat(f)
    case d: Double => bb.putDouble(d)
    case _         => throw new NotImplementedError("Unsupported Type")
  }

  def allocateRaster[T: ClassTag](x: Int, y: Int): Array[Array[T]] = Array.ofDim[T](x, y)

  def getBBGetter[T](bb: ByteBuffer) = (n: T) => n match {
    case b: Byte   => bb.get
    case s: Short  => bb.getShort
    case i: Int    => bb.getInt
    case l: Long   => bb.getLong
    case f: Float  => bb.getFloat
    case d: Double => bb.getDouble
    case _         => throw new NotImplementedError("Unsupported Type")
  }

}