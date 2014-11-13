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

trait RasterTraitExtended extends RasterDataDecodingExtended with RasterDataEncodingExtended {

}

trait RasterTrait extends RasterDataEncoding with RasterDataDecoding {

}

trait RasterDataEncoding extends RasterCommons {

  def encodeFlatRasterToBB[T: Numeric](x: Int, y: Int, raster: Array[T]): ByteBuffer = {
    val bb = allocateRasterBuffer(x*y, raster)
    val setter = getByteBufferSetter(bb)
    appendRasterDimensionality(x, y, bb)
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
    val bb = allocateRasterBuffer(x*y, raster)
    val setter = getByteBufferSetter(bb)
    appendRasterDimensionality(x, y, bb)
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
    val (x, y) = extractRasterDimensionality(bb)
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
    val (x, y) = extractRasterDimensionality(bb)
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
    val (x, y) = extractRasterDimensionality(bb)
    val r = decodeRaster(x, y, bb)
    DenseMatrix.create(x, y, r)
  }

}

trait RasterDataEncodingExtended extends RasterCommons {

  def encodeFlatRasterToBB[T: Numeric](x: Int, y: Int, pt: Int, raster: Array[T]): ByteBuffer = {
    val bb = allocateRasterBuffer(x*y, raster)
    val setter = getByteBufferSetter(bb)
    appendRasterDimAndType(x, y, pt, bb)
    var i = 0
    while (i < raster.length) {
      setter(raster(i))
      i += 1
    }
    bb
  }
  
  def encodeRasterToBB[T: Numeric](x: Int, y: Int, pt: Int, raster: Array[Array[T]]): ByteBuffer = {
    val bb = allocateRasterBuffer(x*y, raster)
    val setter = getByteBufferSetter(bb)
    appendRasterDimAndType(x, y, pt, bb)
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

  def encodeRaster[T: Numeric](x: Int, y: Int, pt: Int, raster: Array[Array[T]]): Array[Byte] = {
    encodeRasterToBB(x, y, pt, raster).array()
  }
  
  def flattenRaster[T: Numeric](raster: Array[Array[T]]): (Int, Int, Array[Byte]) = {
    val (x, y, pt) = getRasterEncodingInfo(raster)
    (x, y, encodeRaster(x, y, pt, raster))
  }
  
  def flattenRasterIncInfo[T: Numeric](raster: Array[Array[T]]): Array[Byte] = {
    val (x, y, pt) = getRasterEncodingInfo(raster)
    encodeRaster(x, y, pt, raster)
  }
  
  def flattenRasterToNIO[T: Numeric](raster: Array[Array[T]]): ByteBuffer = {
    val (x, y, pt) = getRasterEncodingInfo(raster)
    encodeRasterToBB(x, y, pt, raster)
  }

  def flattenRasterToNIO[T: Numeric](x: Int, y: Int, raster: Array[T]): ByteBuffer = {
    val pt = findPrimitiveTypeCode(raster)
    encodeFlatRasterToBB(x, y, pt, raster)
  }


  override val rasterMetaDataSize = 12
  override def allocateRasterBuffer(n: Int, t: Any): ByteBuffer = t match {
    case b: Byte             => ByteBuffer.allocate(n + rasterMetaDataSize)
    case s: Short            => ByteBuffer.allocate((n*2) + rasterMetaDataSize)
    case i: Int              => ByteBuffer.allocate((n*4) + rasterMetaDataSize)
    case l: Long             => ByteBuffer.allocate((n*8) + rasterMetaDataSize)
    case f: Float            => ByteBuffer.allocate((n*4) + rasterMetaDataSize)
    case d: Double           => ByteBuffer.allocate((n*8) + rasterMetaDataSize)
    case a: Array[_]         => allocateRasterBuffer(n, a(0))
    case aa: Array[Array[_]] => allocateRasterBuffer(n, aa(0))
    case _                   => ByteBuffer.allocate((n*8) + rasterMetaDataSize)
  }
  
}

trait RasterDataDecodingExtended extends RasterCommons {
  
  def decodeRaster(x: Int, y:Int, pt: Int, bb: ByteBuffer): Array[AnyVal] = {
    val ret = Array.ofDim[AnyVal](x*y)
    val get = getByteBufferGetter(bb)
    var i = 0
    while (i < x*y) {
      ret.update(i, get(pt))
      i += 1
    }
    ret
  }

  def decodeRasterTo2D(x: Int, y:Int, pt: Int, bb: ByteBuffer): Array[Array[AnyVal]] = {
    val ret = Array.ofDim[AnyVal](x,y)
    val get = getByteBufferGetter(bb)
    var i, j = 0
    while (i < x) {
      while (j < y) {
        ret(i)(j) = get(pt)
        j+=1
      }
      j = 0
      i+= 1
    }
    ret
  }
  
  def upufArrayIncInfo(arr: Array[Byte]): (Int, Int, Int, Array[AnyVal]) = {
    upufNIOIncInfo(ByteBuffer.wrap(arr))
  }
  
  def upufNIOIncInfo(bb: ByteBuffer): (Int, Int, Int, Array[AnyVal]) = {
    val (x, y, pt) = extractRasterDimAndType(bb)
    (x, y, pt, decodeRaster(x, y, pt, bb))
  }

  def upufNIOTo2DArray(bb: ByteBuffer): Array[Array[AnyVal]] = {
    val (x, y, pt) = extractRasterDimAndType(bb)
    decodeRasterTo2D(x, y, pt, bb)
  }

  def upufArrayTo2DArray(arr: Array[Byte]): Array[Array[AnyVal]] = {
    upufNIOTo2DArray(ByteBuffer.wrap(arr))
  }

  //  def upufArrayToDMatrix(arr: Array[Byte]) = {
//    upufNIOToDMatrix(ByteBuffer.wrap(arr))
//  }
  
//  def upufNIOToDMatrix(bb: ByteBuffer) = {
//    val (x, y, pt) = extractRasterDimAndType(bb)
//    val r = decodeRaster(x, y, pt, bb)
//    DenseMatrix.create[AnyVal](x, y, r)
//  }

}

trait RasterCommons {

  val rasterMetaDataSize = 8
  
  def findPrimitiveTypeCode(t: Any): Int = t match {
    case b: Byte             => 0
    case s: Short            => 1
    case i: Int              => 2
    case l: Long             => 3
    case f: Float            => 4
    case d: Double           => 5
    case a: Array[_]         => findPrimitiveTypeCode(a(0))
    case aa: Array[Array[_]] => findPrimitiveTypeCode(aa(0))
    case _                   => -1
  }

  def allocateRasterBuffer(n: Int, t: Any): ByteBuffer = t match {
    case b: Byte             => ByteBuffer.allocate(n + rasterMetaDataSize)
    case s: Short            => ByteBuffer.allocate((n*2) + rasterMetaDataSize)
    case i: Int              => ByteBuffer.allocate((n*4) + rasterMetaDataSize)
    case l: Long             => ByteBuffer.allocate((n*8) + rasterMetaDataSize)
    case f: Float            => ByteBuffer.allocate((n*4) + rasterMetaDataSize)
    case d: Double           => ByteBuffer.allocate((n*8) + rasterMetaDataSize)
    case a: Array[_]         => allocateRasterBuffer(n, a(0))
    case aa: Array[Array[_]] => allocateRasterBuffer(n, aa(0))
    case _                   => ByteBuffer.allocate((n*8) + rasterMetaDataSize)
  }

  def getByteBufferSetter[T](bb: ByteBuffer) = (n: T) => n match {
    case b: Byte   => bb.put(b)
    case s: Short  => bb.putShort(s)
    case i: Int    => bb.putInt(i)
    case l: Long   => bb.putLong(l)
    case f: Float  => bb.putFloat(f)
    case d: Double => bb.putDouble(d)
    case _         => throw new NotImplementedError("Unsupported Type")
  }

  def allocateRasterWithType[T: ClassTag](x: Int, y: Int): Array[Array[T]] = Array.ofDim[T](x, y)

  def allocateRaster(x: Int, y: Int, pt: Int) = pt match {
    case 0 => allocateRasterWithType[Byte](x, y)
    case 1 => allocateRasterWithType[Short](x, y)
    case 2 => allocateRasterWithType[Int](x, y)
    case 3 => allocateRasterWithType[Long](x, y)
    case 4 => allocateRasterWithType[Float](x, y)
    case 5 => allocateRasterWithType[Double](x, y)
    case _ => throw new NotImplementedError("Unsupported Type")
  }

  def getByteBufferGetter(bb: ByteBuffer) = (pt: Int) => pt match {
    case 0 => bb.get
    case 1 => bb.getShort
    case 2 => bb.getInt
    case 3 => bb.getLong
    case 4 => bb.getFloat
    case 5 => bb.getDouble
  }

  def appendRasterDimAndType(x: Int, y: Int, pt: Int, bb: ByteBuffer) = bb.putInt(x).putInt(y).putInt(pt)

  def appendRasterDimensionality(x: Int, y: Int, bb: ByteBuffer) = bb.putInt(x).putInt(y)

  def appendRasterDataType(pt: Int, bb: ByteBuffer) = bb.putInt(pt)

  def extractRasterDimensionality(bb: ByteBuffer): (Int, Int) = (bb.getInt, bb.getInt)

  def extractRasterDataType(bb: ByteBuffer) = bb.getInt

  def extractRasterDimAndType(bb: ByteBuffer): (Int, Int, Int) = (bb.getInt, bb.getInt, bb.getInt)

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

  def getRasterEncodingInfo[T: Numeric](r: Array[Array[T]]): (Int, Int, Int) = {
    val (x, y) = getRasterShape(r)
    val pt = findPrimitiveTypeCode(r)
    (x, y, pt)
  }

}