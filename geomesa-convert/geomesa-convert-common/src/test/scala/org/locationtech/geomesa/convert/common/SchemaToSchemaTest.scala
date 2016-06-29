/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.common

import java.util.{UUID, List => JList}

import com.typesafe.config.ConfigFactory
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureTypeProvider, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

class TestSftProvider extends SimpleFeatureTypeProvider {
  override def loadTypes(): JList[SimpleFeatureType] =  List(TestSftProvider.sftinput)
}

object TestSftProvider {
  val sftinputschema =       """
                               |{
                               |  type-name = "s2stestinput"
                               |  attributes = [
                               |    { name = "lat",      type = "Double", index = false },
                               |    { name = "lon",      type = "Double", index = false },
                               |  ]
                               |}
                             """.stripMargin

  val sftinput = SimpleFeatureTypes.createType(ConfigFactory.parseString(sftinputschema))

}

@RunWith(classOf[JUnitRunner])
class SchemaToSchemaTest extends Specification {

  sequential

  "SchemaToSchemaTest" should {

    val conf = ConfigFactory.parseString(
      """
        | {
        |   type         = "simplefeature",
        |   input-sft    = "s2stestinput",
        |   id-field     = "uuid()",
        |   fields = [
        |     { name = "lat",      transform = "$0" },
        |     { name = "lon",      transform = "$1" },
        |     { name = "geom",     transform = "point($lat, $lon)" },
        |     { name = "something", transform = "concatenate('lat ', toString($0))" }
        |   ]
        | }
      """.stripMargin)

    val sfBuilder = new SimpleFeatureBuilder(TestSftProvider.sftinput)

    val sftoutput = SimpleFeatureTypes.createType(ConfigFactory.parseString(
      """
        |{
        |  type-name = "s2stestoutput"
        |  attributes = [
        |    { name = "lat",      type = "Double", index = false },
        |    { name = "lon",      type = "Double", index = false },
        |    { name = "geom",     type = "Point",  index = true, srid = 4326, default = true },
        |    { name = "something", type = "String", index = false }
        |  ]
        |}
      """.stripMargin
    ))

    def buildTestSF(): SimpleFeature = {
      sfBuilder.reset()
      sfBuilder.set("lat", 12.0)
      sfBuilder.set("lon", 21.0)
      sfBuilder.buildFeature(UUID.randomUUID().toString)
    }

    "transform a simple feature to the target schema" >> {
      val converter = SimpleFeatureConverters.build[SimpleFeature](sftoutput, conf)

      val data = List(buildTestSF())

      val res = converter.processInput(data.toIterator).toList
      res.length mustEqual 1
      res.head.getAttributeCount mustEqual 4
      converter.close()
      success
    }

  }

}
