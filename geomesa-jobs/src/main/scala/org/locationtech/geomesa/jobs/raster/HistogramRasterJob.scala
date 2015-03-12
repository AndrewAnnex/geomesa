/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.jobs.raster

import javax.media.jai.{Histogram, JAI}

import com.twitter.scalding._
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.jobs.raster.RasterJobs._
import org.locationtech.geomesa.jobs.scalding._
import org.locationtech.geomesa.jobs.{GeoMesaBaseJob, JobUtils}

import scala.reflect.ClassTag

object HistogramOperation {

  def kvToHistogram(k: Key, v: Value): Array[Int] = {
    val raster = schema.decode(k, v)
    getHist(raster.chunk)
  }

  def getHist(image: java.awt.image.RenderedImage): Array[Int] = {
    //TODO: make the band selected configurable.
    val dst = JAI.create("histogram", image, null)
    val h = dst.getProperty("histogram").asInstanceOf[Histogram]
    h.getBins(0)
  }

  def addBins[T : Numeric : ClassTag](a: Array[T], b: Array[T]): Array[T] = {
    require(a.length == b.length)
    val op = implicitly[Numeric[T]]
    val ret = new Array[T](a.length)
    var i = 0
    while(i < a.length) {
      ret(i) = op.plus(a(i), b(i))
      i += 1
    }
    ret
  }

}

class HistogramRasterJob(args: Args) extends GeoMesaBaseJob(args) {
  import HistogramRasterJob.HDFS_DEFAULT

  lazy val rdsInParams  = ConnectionParams.toDataStoreInParams(args)
  lazy val input   = AccumuloInputOptions(rdsInParams)
  lazy val output  = args(HDFS_DEFAULT)

  TypedPipe.from(AccumuloSource(input))
    .map { kv: (Key, Value) =>  HistogramOperation.kvToHistogram(kv._1, kv._2) }
    .groupAll.reduce{ (h1: Array[Int], h2: Array[Int]) => HistogramOperation.addBins(h1, h2) }
    .toTypedPipe.map{ p: (Unit, Array[Int]) => s"Histogram = Array(${p._2.mkString(",")})" }
    .write(TypedTsv[String](output))
}

object HistogramRasterJob {

  val HDFS_DEFAULT = "geomesa.hist.raster.hdfs"

  def runJob(conf: Configuration, rdsParams: Map[String, List[String]], hdfs: String): Unit = {
    JobUtils.setLibJars(conf)
    val args: Map[String, List[String]] = Seq(HDFS_DEFAULT -> List(hdfs)).toMap ++ rdsParams
    val instantiateJob = (args: Args) => new HistogramRasterJob(args)
    GeoMesaBaseJob.runJob(conf, args, instantiateJob)
  }

}

