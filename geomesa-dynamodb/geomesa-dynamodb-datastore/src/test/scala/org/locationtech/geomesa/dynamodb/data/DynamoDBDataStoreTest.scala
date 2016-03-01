package org.locationtech.geomesa.dynamodb.data

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.michelboudreau.alternator.AlternatorDB
import com.michelboudreau.alternatorv2.AlternatorDBClientV2
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DynamoDBDataStoreTest extends Specification {

  sequential

  step {
    DynamoDBDataStoreTest.startServer()
  }

  "DynamoDBDataStore" should {

    "allow access" >> {
      val ds = getDataStore
      ds must not(beNull)
      success
    }

    "create a schema" >> {
      val ds = getDataStore
      ds must not(beNull)
      ds.createSchema(SimpleFeatureTypes.createType("test:test", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date"))
      ds.getTypeNames.toSeq must contain("test")
      success
    }

    "fail if no dtg in schema" >> {
      val ds = getDataStore
      val sft = SimpleFeatureTypes.createType("test:nodtg", "name:String,age:Int,*geom:Point:srid=4326")
      ds.createSchema(sft) must throwA[IllegalArgumentException]
      success
    }


  }


  // Teardown, no tests beyond this point
  step {
    DynamoDBDataStoreTest.shutdownServer()
  }

  def initializeDataStore(tableName: String): (DataStore, SimpleFeatureStore) = {
    val ds = getDataStore
    val sft = SimpleFeatureTypes.createType(s"test:$tableName", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date")
    ds.createSchema(sft)

    val gf = JTSFactoryFinder.getGeometryFactory

    val fs = ds.getFeatureSource(s"$tableName").asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(
      DataUtilities.collection(Array(
        SimpleFeatureBuilder.build(sft, Array("john", 10, gf.createPoint(new Coordinate(-75.0, 35.0)), new DateTime("2016-01-01T00:00:00.000Z").toDate).asInstanceOf[Array[AnyRef]], "1"),
        SimpleFeatureBuilder.build(sft, Array("jane", 20, gf.createPoint(new Coordinate(-75.0, 38.0)), new DateTime("2016-01-07T00:00:00.000Z").toDate).asInstanceOf[Array[AnyRef]], "2")
      ))
    )
    (ds, fs)
  }

  def getDataStore: DataStore = {
    import scala.collection.JavaConversions._
    DataStoreFinder.getDataStore(
      Map(
        DynamoDBDataStoreFactory.CATALOG.getName     -> s"ddbTest_${UUID.randomUUID().toString}",
        DynamoDBDataStoreFactory.DYNAMODBAPI.getName -> DynamoDBDataStoreTest.api.getOrElse(throw new Exception("No DynamoDB API class in unit test startup"))
      )
    )
  }

}

object DynamoDBDataStoreTest {

  val client = new AlternatorDBClientV2()

  val storagedir = File.createTempFile("dynamodb","sd")
  storagedir.delete()
  storagedir.mkdir()

  @volatile
  var testDDB: Option[AlternatorDB] = None

  @volatile
  var api: Option[DynamoDB] = None

  private val started = new AtomicBoolean(false)

  def startServer() = {
    if (started.compareAndSet(false, true)) {
      api  = Some(new DynamoDB(client))
      testDDB = Some(new AlternatorDB(9090, storagedir).start())
    }
  }

  def shutdownServer() = {
    if (started.get()) {
      testDDB match {
        case Some(db) =>
          db.stop()
          storagedir.delete()
        case None     =>
      }
    }
  }



}
