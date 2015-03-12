/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.jobs.raster

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.raster.index.RasterIndexSchema
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.collection.mutable

trait RasterJobResources {
  def ds: AccumuloDataStore
  def sft: SimpleFeatureType
  def visibilities: String
  def attributeDescriptors: mutable.Buffer[(Int, AttributeDescriptor)]

  // required by scalding
  def release(): Unit = {}
}

object RasterJobResources {
  import scala.collection.JavaConversions._
  def apply(params:  Map[String, String], feature: String, attributes: List[String]) = new RasterJobResources {
    val ds: AccumuloDataStore = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    val sft: SimpleFeatureType = ds.getSchema(feature)
    val visibilities: String = ds.writeVisibilities
    // the attributes we want to index
    override val attributeDescriptors =
      sft.getAttributeDescriptors
        .zipWithIndex
        .filter { case (ad, idx) => attributes.contains(ad.getLocalName) }
        .map { case (ad, idx) => (idx, ad) }
  }
}

object RasterJobs {
  val schema = RasterIndexSchema
}