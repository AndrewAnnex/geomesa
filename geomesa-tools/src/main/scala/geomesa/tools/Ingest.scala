package geomesa.tools

import com.typesafe.config.ConfigFactory
import geomesa.core.data.AccumuloDataStore
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.{DataStoreFinder, DataUtilities}


import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class Ingest() {}

object Ingest  {
  val conf = ConfigFactory.load()
  val user = conf.getString("tools.user")
  val password = conf.getString("tools.password")
  val instanceId = conf.getString("tools.instanceId")
  val zookeepers = conf.getString("tools.zookeepers")
  val auths = conf.getString("tools.auths")
  val visibilities = conf.getString("tools.visibilities")

  def getAccumuloDataStoreConf(config: Config): HashMap[String, Any] = {
    val dsConfig = HashMap[String, Any]()
    dsConfig.put("instanceId", instanceId)
    dsConfig.put("zookeepers", zookeepers)
    dsConfig.put("user", user)
    dsConfig.put("password", password)
    dsConfig.put("auths", auths)
    dsConfig.put("visibilities", visibilities)
    dsConfig.put("tableName", config.table)
    val instance = new ZooKeeperInstance(instanceId, zookeepers)
    val connector = instance.getConnector(user, new PasswordToken(password))
    dsConfig.put("connector", connector)
    dsConfig
  }

  def defineIngestJob(config: Config) = {
    val dsConfig = getAccumuloDataStoreConf(config)
    println(dsConfig)
    val instance = new ZooKeeperInstance(instanceId, zookeepers)
    val connector = instance.getConnector(user, new PasswordToken(password))
    dsConfig.put("connector", connector)
    val method = config.method
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    method match {
      case "mapreduce" => println("go go mapreduce!")
        try {
          //val sft = ds.getSchema(config.spec)
          //val spec = DataUtilities.encodeType(sft)
          println("Success")
        } catch {
          case t: Throwable => t.printStackTrace()
        }
      case "naive" =>
        try{
          println("go go naive!")
          // get the deliminator for provided type
          lazy val delim = config.format match {
            case "TSV" => "\t"
            case "CSV" => ","
          }





        } catch {
          case t: Throwable => t.printStackTrace()
        }
      case _ => println("Error, no such method exists, no changes made")
    }
  }
}


