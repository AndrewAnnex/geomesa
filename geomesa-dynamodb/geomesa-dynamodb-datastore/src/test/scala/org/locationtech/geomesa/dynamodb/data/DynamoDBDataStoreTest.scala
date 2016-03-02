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
import org.geotools.data.{Query, DataStore, DataStoreFinder, DataUtilities}
import org.geotools.factory.CommonFactoryFinder
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
      ds.dispose()
      ok
    }

    "create a schema" >> {
      val ds = getDataStore
      ds must not(beNull)
      ds.createSchema(SimpleFeatureTypes.createType("test:test", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date"))
      ds.getTypeNames.toSeq must contain("test")
      ds.dispose()
      ok
    }

    "fail if no dtg in schema" >> {
      val ds = getDataStore
      val sft = SimpleFeatureTypes.createType("test:nodtg", "name:String,age:Int,*geom:Point:srid=4326")
      ds.createSchema(sft) must throwA[IllegalArgumentException]
      ds.dispose()
      ok
    }

    "write features" >> {
      val (ds, fs) = initializeDataStore("testwrite")
      val features = fs.getFeatures().features()
      features.toList must haveLength(2)
      features.close()
      ds.dispose()
      ok
    }

    "run bbox between queries" >> {
      val (ds, fs) = initializeDataStore("testbboxbetweenquery")

      val ff = CommonFactoryFinder.getFilterFactory2
      val filter =
        ff.and(ff.bbox("geom", -76.0, 34.0, -74.0, 36.0, "EPSG:4326"),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-08T00:00:00.000Z").toDate)))

      val features = fs.getFeatures(filter).features()
      features.toList must haveLength(1)
      features.close()
      ds.dispose()
      ok
    }

    "run extra-large bbox between queries" >> {
      skipped("intermittent failure")
      val (ds, fs) = initializeDataStore("testextralargebboxbetweenquery")

      val ff = CommonFactoryFinder.getFilterFactory2
      val filt =
        ff.and(ff.bbox("geom", -200.0, -100.0, 200.0, 100.0, "EPSG:4326"),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-01T00:15:00.000Z").toDate)))

      val features = fs.getFeatures(filt).features()
      features.toList must haveLength(1)
      features.close()
      ds.dispose()
      ok
    }.pendingUntilFixed("fixed large bbox query")

    "run bbox between and attribute queries" >> {
      import scala.collection.JavaConversions._

      val (ds, fs) = initializeDataStore("testbboxbetweenandattributequery")
      val ff = CommonFactoryFinder.getFilterFactory2
      val filt =
        ff.and(
          List(
            ff.bbox("geom", -76.0, 34.0, -74.0, 39.0, "EPSG:4326"),
            ff.between(
              ff.property("dtg"),
              ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
              ff.literal(new DateTime("2016-01-08T00:00:00.000Z").toDate)),
            ff.equals(ff.property("name"), ff.literal("jane"))
          )
        )
      val features = fs.getFeatures(filt).features()
      features.toList must haveLength(1)
      features.close()
      ds.dispose()
      ok
    }.pendingUntilFixed("fixed run bbox between and attribute")

    "run poly within and date between queries" >> {
      val (ds, fs) = initializeDataStore("testpolywithinanddtgbetween")

      val gf = JTSFactoryFinder.getGeometryFactory
      val buf = gf.createPoint(new Coordinate(new Coordinate(-75.0, 35.0))).buffer(0.01)
      val ff = CommonFactoryFinder.getFilterFactory2
      val filt =
        ff.and(ff.within(ff.property("geom"), ff.literal(buf)),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-08T00:00:00.000Z").toDate)))

      val features = fs.getFeatures(filt).features()
      features.toList must haveLength(1)
      features.close()
      ds.dispose()
      ok
    }.pendingUntilFixed("fixed run poly within and date between")

    "return correct counts" >> {
      val (ds, fs) = initializeDataStore("testcount")

      val gf = JTSFactoryFinder.getGeometryFactory
      val buf = gf.createPoint(new Coordinate(new Coordinate(-75.0, 35.0))).buffer(0.001)
      val ff = CommonFactoryFinder.getFilterFactory2
      val filt =
        ff.and(ff.within(ff.property("geom"), ff.literal(buf)),
          ff.between(
            ff.property("dtg"),
            ff.literal(new DateTime("2016-01-01T00:00:00.000Z").toDate),
            ff.literal(new DateTime("2016-01-02T00:00:00.000Z").toDate)))

      fs.getCount(new Query("testcount", filt)) mustEqual 1
      ds.dispose()
      ok
    }.pendingUntilFixed("fixed return correct counts")

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
        DynamoDBDataStoreFactory.DYNAMODBAPI.getName -> DynamoDBDataStoreTest.getNewDynamoDB
      )
    )
  }

}

object DynamoDBDataStoreTest {

  val tempDBFile = File.createTempFile("dynamodb","sd")
  tempDBFile.delete()
  tempDBFile.mkdir()

  @volatile
  var server: DynamoDBProxyServer = null

  private val started = new AtomicBoolean(false)

  def startServer() = {
    if (started.compareAndSet(false, true)) {
      System.setProperty("sqlite4java.library.path", "/home/aannex/DynamoDBLocal_lib")
      server = ServerRunner.createServerFromCommandLineArgs(Array("-inMemory", "-sharedDb", "-port", "9305"))
      server.start()
    }
  }

  def shutdownServer() = {
    if (started.get()) {
      if (server != null) server.stop()
      tempDBFile.delete()
    }
  }

  def getNewDynamoDB: DynamoDB = {
    if (started.get()) {
      val d = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""))
      d.setEndpoint("http://localhost:9305")
      new DynamoDB(d)
    } else {
      null
    }
  }



}
