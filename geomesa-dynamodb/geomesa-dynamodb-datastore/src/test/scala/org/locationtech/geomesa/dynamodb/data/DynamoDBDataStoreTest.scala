package org.locationtech.geomesa.dynamodb.data

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.Conversions._
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

    "write features" >> {
      val (ds, fs) = initializeDataStore("testwrite")
      val features = fs.getFeatures().features()
      features.toList must haveLength(2)
      features.close()
      ok
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

  val tempDBFile = File.createTempFile("dynamodb","sd")
  tempDBFile.delete()
  tempDBFile.mkdir()

  @volatile
  var api: Option[DynamoDB] = None

  @volatile
  var server: DynamoDBProxyServer = null

  private val started = new AtomicBoolean(false)

  def startServer() = {
    if (started.compareAndSet(false, true)) {
      System.setProperty("sqlite4java.library.path", "/home/aannex/DynamoDBLocal_lib")
      server = ServerRunner.createServerFromCommandLineArgs(Array("-inMemory", "-port", "9305"))
      server.start()
      val d = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""))
      d.setEndpoint("http://localhost:9305")
      val db = new DynamoDB(d)
      api = Some(db)
    }
  }

  def shutdownServer() = {
    if (started.get()) {
      api match {
        case Some(db) =>
          if (server != null) server.stop()
          tempDBFile.delete()
        case None     =>
      }
    }
  }



}
