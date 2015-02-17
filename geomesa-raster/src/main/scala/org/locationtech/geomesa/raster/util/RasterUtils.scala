/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.raster.util

import java.awt.image.{BufferedImage, Raster => JRaster, RenderedImage}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.{Hashtable => JHashtable}
import javax.media.jai.remote.SerializableRenderedImage

import com.vividsolutions.jts.geom.{Envelope => VEnvelope}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IOUtils, SequenceFile}
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.{GridCoverage2D, GridGeometry2D}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.raster.data.Raster
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.opengis.geometry.Envelope

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

object RasterUtils {

  val coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null)

  object IngestRasterParams {
    val ACCUMULO_INSTANCE   = "geomesa-tools.ingestraster.instance"
    val ZOOKEEPERS          = "geomesa-tools.ingestraster.zookeepers"
    val ACCUMULO_MOCK       = "geomesa-tools.ingestraster.useMock"
    val ACCUMULO_USER       = "geomesa-tools.ingestraster.user"
    val ACCUMULO_PASSWORD   = "geomesa-tools.ingestraster.password"
    val AUTHORIZATIONS      = "geomesa-tools.ingestraster.authorizations"
    val VISIBILITIES        = "geomesa-tools.ingestraster.visibilities"
    val FILE_PATH           = "geomesa-tools.ingestraster.path"
    val HDFS_FILES          = "geomesa-tools.ingestraster.hdfs.files"
    val FORMAT              = "geomesa-tools.ingestraster.format"
    val TIME                = "geomesa-tools.ingestraster.time"
    val GEOSERVER_REG       = "geomesa-tools.ingestraster.geoserver.reg"
    val TABLE               = "geomesa-tools.ingestraster.table"
    val WRITE_MEMORY        = "geomesa-tools.ingestraster.write.memory"
    val WRITE_THREADS       = "geomesa-tools.ingestraster.write.threads"
    val QUERY_THREADS       = "geomesa-tools.ingestraster.query.threads"
    val SHARDS              = "geomesa-tools.ingestraster.shards"
    val PARLEVEL            = "geomesa-tools.ingestraster.parallel.level"
    val CHUNKSIZE           = "geomesa-tools.ingestraster.chunk.size"
    val IS_TEST_INGEST      = "geomesa.tools.ingestraster.is-test-ingest"
  }

  def imageSerialize(image: RenderedImage): Array[Byte] = {
    val buffer: ByteArrayOutputStream = new ByteArrayOutputStream
    val out: ObjectOutputStream = new ObjectOutputStream(buffer)
    val serializableImage = new SerializableRenderedImage(image, true)
    try {
      out.writeObject(serializableImage)
    } finally {
      out.close
    }
    buffer.toByteArray
  }

  def imageDeserialize(imageBytes: Array[Byte]): RenderedImage = {
    val in: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(imageBytes))
    var read: RenderedImage = null
    try {
      read = in.readObject.asInstanceOf[RenderedImage]
    } finally {
      in.close
    }
    read
  }

  def allocateBufferedImage(width: Int, height: Int, chunk: RenderedImage): BufferedImage = {
    val properties = new JHashtable[String, Object]
    if (chunk.getPropertyNames != null) {
      chunk.getPropertyNames.foreach(name => properties.put(name, chunk.getProperty(name)))
    }
    val colorModel = chunk.getColorModel
    val alphaPremultiplied = colorModel.isAlphaPremultiplied
    val sampleModel = chunk.getSampleModel.createCompatibleSampleModel(width, height)
    val emptyRaster = JRaster.createWritableRaster(sampleModel, null)
    new BufferedImage(colorModel, emptyRaster, alphaPremultiplied, properties)
  }

  def renderedImageToBufferedImage(r: RenderedImage): BufferedImage = {
    val properties = new JHashtable[String, Object]
    if (r.getPropertyNames != null) {
      r.getPropertyNames.foreach(name => properties.put(name, r.getProperty(name)))
    }
    val colorModel = r.getColorModel
    val alphaPremultiplied = colorModel.isAlphaPremultiplied
    val sampleModel = r.getSampleModel
    new BufferedImage(colorModel, r.copyData(null), alphaPremultiplied, properties)
  }

  def getEmptyImage(width: Int, height: Int, imageType: Int): BufferedImage = {
    new BufferedImage(width, height, imageType)
  }

  def simpleWriteToMosaic(mosaic: BufferedImage, raster: Raster, env: VEnvelope, resX: Double, resY: Double) = {
    val rasterEnv = raster.referencedEnvelope
    val originX = Math.floor((rasterEnv.getMinX - env.getMinX) / resX).toInt
    val originY = Math.floor((env.getMaxY - rasterEnv.getMaxY) / resY).toInt
    mosaic.getRaster.setRect(originX, originY, raster.chunk.getData)
  }

  def cascadeBoundingBoxes(bboxes: List[BoundingBox]): BoundingBox = bboxes.reduce( (a, b) => BoundingBox.getCoveringBoundingBox(a, b) )

  def vividToGeotools(e: VEnvelope): ReferencedEnvelope = {
    new ReferencedEnvelope(e.getMinX, e.getMaxX, e.getMinY, e.getMaxY, DefaultGeographicCRS.WGS84)
  }

  def mosaicChunks(chunks: Iterator[Raster], queryWidth: Int, queryHeight: Int, queryEnv: Envelope): (BufferedImage, Int, Envelope) = {
    // TODO: Add check for Iterator with only a single Raster. https://geomesa.atlassian.net/browse/GEOMESA-671
    if (chunks.isEmpty) {
      (getEmptyImage(queryWidth, queryHeight, BufferedImage.TYPE_BYTE_GRAY), 0, queryEnv)
    } else {
      val theChunks = chunks.toList
      val count = theChunks.length
      val chunkBounds = cascadeBoundingBoxes(theChunks.map(_.BBox))
      val chunkBoundsEnv = vividToGeotools(chunkBounds.envelope)
      if (count <= 1) {
        (renderedImageToBufferedImage(theChunks.head.chunk), count, chunkBoundsEnv)
      } else {
        val compositeEnv: VEnvelope = chunkBounds.envelope
        val accumuloRasterXRes = theChunks.head.referencedEnvelope.getSpan(0) / theChunks.head.chunk.getWidth
        val accumuloRasterYRes = theChunks.head.referencedEnvelope.getSpan(1) / theChunks.head.chunk.getHeight
        val compositeX = (chunkBounds.getWidth / accumuloRasterXRes).toInt
        val compositeY = (chunkBounds.getHeight / accumuloRasterYRes).toInt
        val mosaic = allocateBufferedImage(compositeX, compositeY, theChunks.head.chunk)
        theChunks.par.foreach{ chunk =>
          simpleWriteToMosaic(mosaic, chunk, compositeEnv, accumuloRasterXRes, accumuloRasterYRes)
        }
        (mosaic, count, chunkBoundsEnv)
      }
    }
  }

  def mosaicChunksToCoverage(chunks: Iterator[Raster], queryWidth: Int, queryHeight: Int,
                             queryEnv: Envelope, name: String): (GridCoverage2D, Int) = {
    val mosaicTuple = mosaicChunks(chunks, queryWidth, queryHeight, queryEnv)
    val coverage = coverageFactory.create(name, mosaicTuple._1, mosaicTuple._3)
    mosaicTuple._1.flush()
    (coverage, mosaicTuple._2)
  }

  def envelopeToReferencedEnvelope(e: Envelope): ReferencedEnvelope = {
    new ReferencedEnvelope(e.getMinimum(0),
      e.getMaximum(0),
      e.getMinimum(1),
      e.getMaximum(1),
      DefaultGeographicCRS.WGS84)
  }

  def getNewImage[T: TypeTag](w: Int, h: Int, fill: Array[T],
                              imageType: Int = BufferedImage.TYPE_BYTE_GRAY): BufferedImage = {
    val image = new BufferedImage(w, h, imageType)
    val wr = image.getRaster
    val setPixel: (Int, Int) => Unit = typeOf[T] match {
      case t if t =:= typeOf[Int]    =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Int]])
      case t if t =:= typeOf[Float]  =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Float]])
      case t if t =:= typeOf[Double] =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Double]])
      case _                         =>
        throw new IllegalArgumentException(s"Error, cannot handle Arrays of type: ${typeOf[T]}")
    }

    for (i <- 0 until h; j <- 0 until w) { setPixel(i, j) }
    image
  }

  case class sharedRasterParams(gg: GridGeometry2D, envelope: Envelope) {
    val width = gg.getGridRange2D.getWidth
    val height = gg.getGridRange2D.getHeight
    val resX = (envelope.getMaximum(0) - envelope.getMinimum(0)) / width
    val resY = (envelope.getMaximum(1) - envelope.getMinimum(1)) / height
    val suggestedQueryResolution = math.min(resX, resY)
  }

  def getSequenceFileWriter(outFile: String, conf: Configuration): SequenceFile.Writer = {
    val outPath = new Path(outFile)
    val key = new BytesWritable
    val value = new BytesWritable
    try {
      val optPath = SequenceFile.Writer.file(outPath)
      val optKey =  SequenceFile.Writer.keyClass(key.getClass)
      val optVal =  SequenceFile.Writer.valueClass(value.getClass)
      SequenceFile.createWriter(conf, optPath, optKey, optVal)
    } catch {
      case e: Exception =>
        throw new Exception("Cannot create writer on Hdfs sequence file: " + e.getMessage())
    }
  }

  def saveBytesToHdfsFile(name: String, bytes: Array[Byte], writer: SequenceFile.Writer) {
    writer.append(new BytesWritable(name.getBytes), new BytesWritable(bytes))
  }

  def closeSequenceWriter(writer:  SequenceFile.Writer) {
    IOUtils.closeStream(writer)
  }

  //Encode a list of byte arrays into one byte array using protocol: length | data
  //Result is like: length[4 bytes], byte array, ... [length[4 bytes], byte array]
  def encodeByteArrays(bas: List[Array[Byte]]): Array[Byte] =  {
    val totalLength = bas.map(_.length).sum
    val buffer = ByteBuffer.allocate(totalLength + 4 * bas.length)
    bas.foreach{ ba => buffer.putInt(ba.length).put(ba) }
    buffer.array
  }

  //Decode a byte array into a list of byte array using protocol: length | data
  def decodeByteArrays(ba: Array[Byte], numToExtract: Int): List[Array[Byte]] = {
    var pos = 0
    var num = 1
    val listBuf: ListBuffer[Array[Byte]] = new ListBuffer[Array[Byte]]()
    while(num <= numToExtract && pos + 4 <= ba.length) {
      val length = ByteBuffer.wrap(ba, pos, 4).getInt
      listBuf += ba.slice(pos + 4, pos + 4 + length)
      pos = pos + 4 + length
      num += 1
    }
    listBuf.toList
  }

  val doubleSize = 8
  def doubleToBytes(d: Double): Array[Byte] = {
    val bytes = new Array[Byte](doubleSize)
    ByteBuffer.wrap(bytes).putDouble(d)
    bytes
  }

  def bytesToDouble(bs: Array[Byte]): Double = ByteBuffer.wrap(bs).getDouble
}

