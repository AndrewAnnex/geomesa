/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging

class Ingest() extends Logging {

  def getAccumuloDataStoreConf(config: Config) = Map (
    "instanceId"   ->  sys.env.getOrElse("GEOMESA_INSTANCEID", "dcloud"),
    "zookeepers"   ->  sys.env.getOrElse("GEOMESA_ZOOKEEPERS", "dzoo1:2181,dzoo2:2181,dzoo3:2181"),
    "user"         ->  sys.env.getOrElse("GEOMESA_USER", "root"),
    "password"     ->  sys.env.getOrElse("GEOMESA_PASSWORD", "secret"),
    "auths"        ->  sys.env.getOrElse("GEOMESA_AUTHS", ""),
    "visibilities" ->  sys.env.getOrElse("GEOMESA_VISIBILITIES", ""),
    "tableName"    ->  config.table
  )

  def defineIngestJob(config: Config): Boolean = {
    val dsConfig = getAccumuloDataStoreConf(config)
    config.format.toUpperCase match {
      case "CSV" | "TSV" =>
        config.method.toLowerCase match {
          case "mapreduce" =>
            true
          case "naive" =>
            new SVIngest(config, dsConfig)
            true
          case _ =>
            logger.error("Error, no such ingest method for CSV or TSV found, no data ingested")
            false
        }
      case "GEOJSON" | "JSON" =>
        config.method.toLowerCase match {
          case "naive" =>
            new GeoJsonIngest(config, dsConfig)
            true
          case _ =>
            logger.error("Error, no such ingest method for GEOJSON or JSON found, no data ingested")
            false
        }
      case "GML" | "KML" =>
        config.method.toLowerCase match {
          case "naive" =>
            true
          case _ =>
            logger.error("Error, no such ingest method for GML or KML found, no data ingested")
            false
        }
      case "SHAPEFILE" | "SHP" =>
        config.method.toLowerCase match {
          case "naive" =>
            true
          case _ =>
            logger.error("Error, no such ingest method for Shapefiles found, no data ingested")
            false
        }
      case _ =>
        logger.error(s"Error, format: \'${config.format}\' not supported. Supported formats include: CSV, TSV, GEOJSON, JSON, GML, KML, SHAPEFILE ")
        false
    }
  }
}