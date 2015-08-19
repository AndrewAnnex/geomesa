/** *********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  * ************************************************************************/

package org.locationtech.geomesa.accumulo.process.proximity

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.DataStoreFinder
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ProximitySearchProcessTest extends Specification {

  sequential

  val dtgField = org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD
  val geotimeAttributes = s"*geom:Geometry:srid=4326,$dtgField:Date"

  def createStore: AccumuloDataStore = {
    // the specific parameter values should not matter, as we
    // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user" -> "myuser",
      "password" -> "mypassword",
      "auths" -> "A,B,C",
      "tableName" -> "testwrite",
      "useMock" -> "true",
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]
  }

  val sftName = "geomesaProximityTestType"
  val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")
  sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

  val ds = createStore

  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val featureCollection = new DefaultFeatureCollection(sftName, sft)

  List("a", "b").foreach { name =>
    List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
      val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute(org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      sf.setAttribute("type", name)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
  }

  // write the feature to the store
  val res = fs.addFeatures(featureCollection)

  val geoFactory = JTSFactoryFinder.getGeometryFactory

  def getPoint(lat: Double, lon: Double, meters: Double) =
    GeometryUtils.farthestPoint(geoFactory.createPoint(new Coordinate(lat, lon)), meters)

  "GeomesaProximityQuery" should {
    "find things close by" in {
      import org.locationtech.geomesa.utils.geotools.Conversions._
      val p1 = getPoint(45, 45, 99)
      WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) must beFalse
      val p2 = getPoint(46, 46, 99)
      val p3 = getPoint(47, 47, 99)


      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      List(1, 2, 3).zip(List(p1, p2, p3)).foreach { case (i, p) =>
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute(org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
        sf.setAttribute("type", "fake")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        inputFeatures.add(sf)
      }

      val dataFeatures = fs.getFeatures()

      dataFeatures.size should be equalTo 8
      val prox = new ProximitySearchProcess
      prox.execute(inputFeatures, dataFeatures, 50.0).size should be equalTo 0
      prox.execute(inputFeatures, dataFeatures, 90.0).size should be equalTo 0
      prox.execute(inputFeatures, dataFeatures, 99.1).size should be equalTo 6
      prox.execute(inputFeatures, dataFeatures, 100.0).size should be equalTo 6
      prox.execute(inputFeatures, dataFeatures, 101.0).size should be equalTo 6
    }

    "work with a linestring and buffer of 50meters" in {
      val sftNameDataPoints = "geomesaProximityTestTypeDataPoints"
      val sftDataPoints = SimpleFeatureTypes.createType(sftNameDataPoints, s"type:String,$geotimeAttributes")
      val sftNameLineData = "geomesaProximityTestTypeLineData"
      val sftLineData = SimpleFeatureTypes.createType(sftNameLineData, s"type:String,$geotimeAttributes")

      val time = new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate
      val dtg = org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD

      val queryLine = WKTUtils.read("LINESTRING (-78.49516 38.07573, -78.48304 38.02478)")
      // Coord constructor takes (x, y) (lon, lat)
      val includedPoints = List(new Coordinate(-78.492831, 38.065319), new Coordinate(-78.484892, 38.032880), new Coordinate(-78.483924, 38.028251)).map(geoFactory.createPoint)
      val excludedPoints = List(new Coordinate(-78.495658, 38.056217), new Coordinate(-78.484331, 38.033467), new Coordinate(-78.477297, 38.026967)).map(geoFactory.createPoint)

      // define the data to query against
      val dataFeatures = new DefaultFeatureCollection(sftNameDataPoints, sftDataPoints)
      includedPoints.zip(excludedPoints).zipWithIndex.foreach { case ((l, r), i) =>
        val sfInclude = AvroSimpleFeatureFactory.buildAvroFeature(sftDataPoints, List.empty, s"${i}include")
        sfInclude.setDefaultGeometry(l)
        sfInclude.setAttribute(dtg, time)
        sfInclude.setAttribute("type", "fake")
        dataFeatures.add(sfInclude)
        val sfExclude = AvroSimpleFeatureFactory.buildAvroFeature(sftDataPoints, List.empty, s"${i}exclude")
        sfExclude.setDefaultGeometry(r)
        sfExclude.setAttribute(dtg, time)
        sfExclude.setAttribute("type", "fake")
        dataFeatures.add(sfExclude)
      }

      // define the feature that will be buffered for the query
      val inputFeature = new DefaultFeatureCollection(sftNameLineData, sftLineData)
      val sfLine = AvroSimpleFeatureFactory.buildAvroFeature(sftLineData, List.empty, "1query")
      sfLine.setDefaultGeometry(queryLine)
      sfLine.setAttribute(dtg, time)
      sfLine.setAttribute("type", "fale")
      inputFeature.add(sfLine)

      //
      dataFeatures.size should be equalTo 6
      inputFeature.size should be equalTo 1
      val prox = new ProximitySearchProcess
      prox.execute(inputFeature, dataFeatures, 1.0).size mustEqual 0
      prox.execute(inputFeature, dataFeatures, 50.0).size mustEqual 3
      prox.execute(inputFeature, dataFeatures, 750.0).size mustEqual 6
    }
  }

  "GeomesaProximityQuery" should {
    "work on non-accumulo feature sources" in {
      import org.locationtech.geomesa.utils.geotools.Conversions._
      val p1 = getPoint(45, 45, 99)
      WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) must beFalse
      val p2 = getPoint(46, 46, 99)
      val p3 = getPoint(47, 47, 99)


      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      List(1, 2, 3).zip(List(p1, p2, p3)).foreach { case (i, p) =>
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute(org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
        sf.setAttribute("type", "fake")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        inputFeatures.add(sf)
      }

      val nonAccumulo = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          nonAccumulo.add(sf)
        }
      }

      nonAccumulo.size should be equalTo 8
      val prox = new ProximitySearchProcess
      prox.execute(inputFeatures, nonAccumulo, 30.0).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 98.0).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 99.0001).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 100.0).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 101.0).size should be equalTo 6
    }
  }

}
