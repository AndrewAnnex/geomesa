/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.jobs.raster

import java.awt.image.RenderedImage

import com.twitter.scalding.Args
import com.twitter.scalding.TypedPipe
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.jobs.raster.RasterJobs._
import org.locationtech.geomesa.jobs.scalding._
import org.locationtech.geomesa.jobs.{GeoMesaBaseJob, JobUtils}
import org.locationtech.geomesa.raster.data.Raster

object GrayScaleOperation {
  val d = Array(Array(.21, .71, 0.07, 0.0))

  def colorKVtoGrayScaleMutation(k: Key, v: Value): Mutation = {
    val raster = schema.decode(k, v)

    import javax.media.jai._
    val grayScale: RenderedImage = JAI.create("bandcombine", raster.chunk, d, null)

    val grayRaster = Raster(grayScale, raster.metadata, raster.resolution)

    val (nk, nv) = schema.encode(grayRaster)

    val m = new Mutation(nk.getRow)
    m.put(nk.getColumnFamily, nk.getColumnQualifier, nk.getColumnVisibilityParsed, nv)
    m
  }

}

class GrayScaleRasterJob(args: Args) extends GeoMesaBaseJob(args) {

  lazy val rdsInParams  = ConnectionParams.toDataStoreInParams(args)
  lazy val rdsOutParams = rdsInParams ++ ConnectionParams.toDataStoreOutParams(args)

  val input:  AccumuloInputOptions   = AccumuloInputOptions(rdsInParams)
  val output: AccumuloOutputOptions  = AccumuloOutputOptions(rdsOutParams)

  TypedPipe.from(AccumuloSource(input)).map{ kv: (Key, Value) =>
    (null: Text, GrayScaleOperation.colorKVtoGrayScaleMutation(kv._1, kv._2))
  }.write(AccumuloSource(output))

}

object GrayScaleRasterJob {

  def runJob(conf: Configuration, rdsParams: Map[String, List[String]]): Unit = {
    JobUtils.setLibJars(conf)
    val instantiateJob = (args: Args) => new GrayScaleRasterJob(args)
    GeoMesaBaseJob.runJob(conf, rdsParams, instantiateJob)
  }

}