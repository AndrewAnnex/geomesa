/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.accumulo.data

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.{Query, DataStoreFinder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.iterators.TestData
import org.locationtech.geomesa.accumulo.iterators.TestData._
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class RemoveSchemaTest extends Specification with Logging {

  "RemoveSchema in AccumuloDataStore" should {
    "Delete the z3 table after being called" in {
      val tableName = "RemoveSchemaTest"

      val mockInstance = new MockInstance("mycloudRemoveSchema")
      val c = mockInstance.getConnector("myuser", new PasswordToken("mypassword".getBytes("UTF8")))

      val dsParams = Map(
        "instanceId" -> "mycloud",
        "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user" -> "myuser",
        "password" -> "mypassword",
        "auths" -> "A,B,C",
        "tableName" -> tableName,
        "useMock" -> "true")

      val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

      val sft = TestData.getFeatureType("1", tableSharing = true)
      ds.createSchema(sft)

      // Assert that the data store has our sft
      ds.getSchema(sft.getTypeName) must not beNull

      // write some data to the ds
      val someData = mediumData.map(createSF(_, sft))
      val fs = getFeatureStore(ds, sft, someData)

      // assert we wrote something
      val count = ds.getFeatureSource(sft.getTypeName).getCount(new Query(sft.getTypeName, Filter.INCLUDE))
      count must beGreaterThan(0)

      // assert that the z table exists
      val z3TableName = ds.getTableName(sft.getTypeName, Z3Table)
      c.tableOperations().exists(z3TableName) must beTrue

      // call remove schema
      ds.removeSchema(sft.getTypeName)

      // assert that the z table does not exist
      c.tableOperations().exists(z3TableName) must beFalse
    }
  }


}
