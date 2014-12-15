package org.locationtech.geomesa.raster.util

import java.awt.image.{BufferedImage, RenderedImage, WritableRaster}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.Charset
import javax.media.jai.remote.SerializableRenderedImage

import org.geotools.coverage.grid.{GridCoverage2D, GridCoverageFactory}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.data.{RasterQuery, RasterStore}
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.opengis.geometry.Envelope

import scala.reflect.runtime.universe._
import scala.{specialized => sp}

object RasterUtils {
  val doubleSize = 8
  implicit def doubleToBytes(d: Double): Array[Byte] = {
    val bytes = new Array[Byte](doubleSize)
    ByteBuffer.wrap(bytes).putDouble(d)
    bytes
  }
  implicit def bytesToDouble(bs: Array[Byte]): Double = ByteBuffer.wrap(bs).getDouble

  val intSize = 4
  implicit def intToBytes(i: Int): Array[Byte] = {
    val bytes = new Array[Byte](intSize)
    ByteBuffer.wrap(bytes).putInt(i)
    bytes
  }
  implicit def bytesToInt(bs: Array[Byte]): Int = ByteBuffer.wrap(bs).getInt

  val longSize = 8
  implicit def longToBytes(l: Long): Array[Byte] = {
    val bytes = new Array[Byte](longSize)
    ByteBuffer.wrap(bytes).putLong(l)
    bytes
  }
  implicit def bytesToLong(bs: Array[Byte]): Long = ByteBuffer.wrap(bs).getLong

  val utf8Charset = Charset.forName("UTF-8")
  implicit def stringToBytes(s: String): Array[Byte] = s.getBytes(utf8Charset)
  implicit def bytesToString(bs: Array[Byte]): String = new String(bs, utf8Charset)

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

  val defaultGridCoverageFactory = new GridCoverageFactory

  def renderedImageToGridCoverage2d(name: String, image: RenderedImage, env: Envelope): GridCoverage2D =
    defaultGridCoverageFactory.create(name, image, env)

  val white = Array[Int] (255, 255, 255)
  val black = Array[Int] (0, 0, 0)


  //[T : Numeric : ClassTag]
  def getNewImage[T: TypeTag](w: Int, h: Int, fill: Array[T], imageType: Int = BufferedImage.TYPE_BYTE_GRAY): BufferedImage = {
    val image = new BufferedImage(w, h, imageType)
    val wr = image.getRaster
    for (i <- 1 until h) {
      for (j <- 1 until w) {
        typeOf[T] match {
          case t if t =:= typeOf[Int]    => wr.setPixel(j, i, fill.asInstanceOf[Array[Int]])
          case t if t =:= typeOf[Float]  => wr.setPixel(j, i, fill.asInstanceOf[Array[Float]])
          case t if t =:= typeOf[Double] => wr.setPixel(j, i, fill.asInstanceOf[Array[Double]])
        }
      }
    }
    image
  }

  def imageToCoverage(img: WritableRaster, env: ReferencedEnvelope, cf: GridCoverageFactory) = {
    cf.create("testRaster", img, env)
  }

  def createTestRasterStore(tableName: String) = {
    val rs = RasterStore("user", "pass", "testInstance", "zk", tableName, "SUSA", "SUSA", true)
    rs
  }

  def generateQuery(minX: Int, maxX:Int, minY: Int, maxY: Int, res: Double = 10.0) = {
    val bb = BoundingBox(new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84))
    new RasterQuery(bb, res, None, None)
  }
  
  def generateTestGridCoverage(minX: Int, maxX:Int, minY: Int, maxY: Int, w: Int, h: Int, fill: Array[Int] = black) = {
    val env = new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84)
    val image = getNewImage(w, h, fill)
    
    imageToCoverage(image.getRaster(), env, defaultGridCoverageFactory)
  }

  def generateTestRaster(minX: Int, maxX:Int, minY: Int, maxY: Int, w: Int = 256, h: Int = 256, res: Double = 10.0) = {
    val ingestTime = new DateTime()
    val bbox = BoundingBox(minX, maxX, minY, maxY)
    val metadata = DecodedIndex(Raster.getRasterId("testRaster"), bbox.geom, Option(ingestTime.getMillis))
    val coverage = generateTestGridCoverage(minX, maxX, minY, maxY, w, h)
    new Raster(coverage.getRenderedImage, metadata, res)
  }

}

