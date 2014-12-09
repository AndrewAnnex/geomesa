package org.locationtech.geomesa.raster.data

import java.awt.image._

import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.GridCoverageFactory
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
//import org.jaitools.imageutils.ImageUtils
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.index.DecodedIndex
//import org.locationtech.geomesa.plugin.ImageUtils
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner



@RunWith(classOf[JUnitRunner])
class RasterStoreTest extends Specification {

  def createAndFillRasterStore = {

    val rs = RasterStore("user", "pass", "testInstance", "zk", "testTable", "SUSA", "SUSA", true)

    val rasterName = "testRaster"

    val ingestTime = new DateTime()

    val coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(new Hints())

    val env = new ReferencedEnvelope(0, 50, 0, 50, DefaultGeographicCRS.WGS84)

    val bbox = BoundingBox(env)

    val metadata = DecodedIndex(Raster.getRasterId(rasterName), bbox.geom, Option(ingestTime.getMillis))

    val image = getNewImage(500, 500, Array[Int](255, 255, 255))

    val coverage = imageToCoverage(500, 500, image.getRaster(), env, coverageFactory)

    val raster = new Raster(coverage.getRenderedImage, metadata, 10.0)

    rs.putRaster(raster)
    rs
  }

  def generateQuery = {
    val bb = BoundingBox(new ReferencedEnvelope(0, 50, 0, 50, DefaultGeographicCRS.WGS84))
    new RasterQuery(bb, 10, None, None)
  }

  // stolen from elsewhere
  def getNewImage(width: Int, height: Int, color: Array[Int]): BufferedImage = {
    val image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    var h = 0
    var w = 0
    for (h <- 1 until height) {
      for (w <- 1 until width) {
        wr.setPixel(w, h, color)
      }
    }
    image
  }

  def imageToCoverage(width: Int, height: Int, img: WritableRaster, env: ReferencedEnvelope, cf: GridCoverageFactory) = {
    cf.create("testRaster", img, env)
  }

  "RasterStore" should {
    "create a Raster Store" in {
      val theStore = createAndFillRasterStore
      theStore must beAnInstanceOf[RasterStore]
      val theIterator = theStore.getRasters(generateQuery)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }
  }





}
