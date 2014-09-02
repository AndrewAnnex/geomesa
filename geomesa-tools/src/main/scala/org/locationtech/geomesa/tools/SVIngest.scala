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
package org.locationtech.geomesa.tools

import java.io.{BufferedReader, File, FileReader}
import java.net.URLDecoder
import java.nio.charset.Charset

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import org.geotools.data.{DataStoreFinder, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.index.Constants
import org.locationtech.geomesa.feature.{AvroSimpleFeature, AvroSimpleFeatureFactory}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.{Failure, Success, Try}

class SVIngest(config: IngestArguments, dsConfig: Map[String, _]) extends Logging {

  import scala.collection.JavaConversions._

  lazy val idFields         = config.idFields.orNull
  lazy val path             = config.file
  lazy val featureName      = config.featureName.get
  lazy val sftSpec          = URLDecoder.decode(config.spec, "UTF-8")
  lazy val dtgField         = config.dtField
  lazy val dtgFmt           = config.dtFormat
  lazy val dtgTargetField   = sft.getUserData.get(Constants.SF_PROPERTY_START_TIME).asInstanceOf[String]
  lazy val latField         = config.latAttribute.orNull
  lazy val lonField         = config.lonAttribute.orNull
  lazy val skipHeader       = config.skipHeader
  lazy val doHash           = config.doHash
  var lineNumber            = 0
  var failures              = 0
  var successes             = 0
  val maxShard: Option[Int] = config.maxShards

  lazy val dropHeader = skipHeader match {
    case true => 1
    case _    => 0
  }

  val csvFormat =
    config.format.get.toUpperCase match {
      case "TSV" => CSVFormat.TDF
      case "CSV" => CSVFormat.DEFAULT
    }

  val delim = csvFormat.withSkipHeaderRecord(skipHeader)

  val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]

  if (ds.getSchema(featureName) == null) {
    logger.info("\tCreating GeoMesa tables...")
    val startTime = System.currentTimeMillis()
    if (maxShard.isDefined)
      ds.createSchema(sft, maxShard.get)
    else
      ds.createSchema(sft)
    val createTime = System.currentTimeMillis() - startTime
    val numShards = ds.getSpatioTemporalMaxShard(sft)
    val shardPvsS = if (numShards == 1) "Shard" else "Shards"
    logger.info(s"\tCreated schema in: $createTime ms using $numShards $shardPvsS.")
  } else {
    val numShards = ds.getSpatioTemporalMaxShard(sft)
    val shardPvsS = if (numShards == 1) "Shard" else "Shards"
    maxShard match {
      case None => logger.info(s"GeoMesa tables and SFT extant, using $numShards $shardPvsS. " +
        s"\nIf this is not desired please delete (aka: drop) the catalog using the delete command.")
      case Some(x) => logger.warn(s"GeoMesa tables extant, ignoring user request, using schema's $numShards $shardPvsS")
    }
  }

  lazy val sft = {
    val ret = SimpleFeatureTypes.createType(featureName, sftSpec)
    ret.getUserData.put(Constants.SF_PROPERTY_START_TIME, dtgField.getOrElse(Constants.SF_PROPERTY_START_TIME))
    ret
  }

  lazy val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
  lazy val geomFactory = JTSFactoryFinder.getGeometryFactory
  lazy val dtFormat = DateTimeFormat.forPattern(dtgFmt.getOrElse("MILLISEPOCH"))
  lazy val attributes = sft.getAttributeDescriptors
  lazy val dtBuilder = dtgField.flatMap(buildDtBuilder)
  lazy val idBuilder = buildIDBuilder

  // This class is possibly necessary for scalding (to be added later)
  // Otherwise it can be removed with just the line val fw = ... retained
  class CloseableFeatureWriter {
    val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    def release(): Unit = { fw.close() }
  }

  def runIngest() = {
    config.method.toLowerCase match {
      case "local" =>
        val cfw = new CloseableFeatureWriter
        if ( dtgField.isEmpty ) {
          // assume we have no user input on what date field to use and that
          // there is no column of data signifying it.
          logger.warn("Warning: no date-time field specified. Assuming that data contains no date column. \n" +
            s"GeoMesa is defaulting to the system time for ingested features.")
        }
        try {
          performIngest(cfw, new File(path))
        } catch {
          case e: Exception => logger.error("error", e)
        }
        finally {
          cfw.release()
          ds.dispose()
          val successPvsS = if (successes == 1) "feature" else "features"
          val failurePvsS = if (failures == 1) "feature" else "features"
          val failureString = if (failures == 0) "with no failures" else s"and failed to ingest: $failures $failurePvsS"
          logger.info(s"For file $path - ingested: $successes $successPvsS, $failureString.")
        }
      case _ =>
        logger.error(s"Error, no such SV ingest method: ${config.method.toLowerCase}")
    }
  }

  def runTestIngest(file: File) = Try {
    val cfw = new CloseableFeatureWriter
    performIngest(cfw, file)
    cfw.release()
  }

  def performIngest(cfw: CloseableFeatureWriter, file: File) = {
    linesToFeatures(file).foreach {
      case Success(ft) =>
        writeFeature(cfw.fw, ft)
        // Log info to user that ingest is still working, might be in wrong spot however...
        if ( lineNumber % 10000 == 0 ) {
          val successPvsS = if (successes == 1) "feature" else "features"
          val failurePvsS = if (failures == 1) "feature" else "features"
          val failureString = if (failures == 0) "with no failures" else s"and failed to ingest: $failures $failurePvsS"
          logger.info(s"Ingest proceeding, on line number: $lineNumber," +
            s" ingested: $successes $successPvsS, $failureString.")
        }
      case Failure(ex) => failures +=1; logger.error(s"Could not write feature on " +
        s"line number: $lineNumber due to: ${ex.getLocalizedMessage}")
    }
  }

  def linesToFeatures(file: File) = {
    val csvParser = new CSVParser(new BufferedReader(new FileReader(file)), delim)
    for(line <- csvParser) yield lineToFeature(line)
  }

  def lineToFeature(record: CSVRecord): Try[AvroSimpleFeature] = Try {
    lineNumber += 1

    val fields = record.toSeq

    val id = idBuilder(fields)
    builder.reset()
    builder.addAll(fields)
    val feature = builder.buildFeature(id).asInstanceOf[AvroSimpleFeature]

    dtBuilder.foreach { dateBuilder => addDateToFeature(record, fields, feature, dateBuilder) }
    // Support for point data method
    val lon = Option(feature.getAttribute(lonField)).map(_.asInstanceOf[Double])
    val lat = Option(feature.getAttribute(latField)).map(_.asInstanceOf[Double])
    (lon, lat) match {
      case (Some(x), Some(y)) => feature.setDefaultGeometry(geomFactory.createPoint(new Coordinate(x, y)))
      case _                  => Nil
    }

    feature
  }

  def addDateToFeature(record: CSVRecord, fields: Seq[String], feature: AvroSimpleFeature,
                       dateBuilder: (AnyRef) => DateTime) {
    try {
      val dtgFieldIndex = getAttributeIndexInLine(dtgField.get)
      val date = dateBuilder(fields(dtgFieldIndex)).toDate
      feature.setAttribute(dtgField.get, date)
    } catch {
      case e: Exception => throw new Exception(s"Could not form Date object from field" +
        s" using dt-format: $dtgFmt, on line number: $lineNumber \n\t With value of: $record")
    }
    //now try to build the date time object and set the dtgTargetField to the date value
    val dtg = try {
      dateBuilder(feature.getAttribute(dtgField.get))
    } catch {
      case e: Exception => throw new Exception(s"Could not find date-time field: '$dtgField'," +
        s" on line  number: $lineNumber \n\t With value of: $record")
    }

    feature.setAttribute(dtgTargetField, dtg.toDate)
  }

  def writeFeature(fw: FeatureWriter[SimpleFeatureType, SimpleFeature], feature: AvroSimpleFeature) = {
    try {
      val toWrite = fw.next()
      sft.getAttributeDescriptors.foreach { ad =>
        toWrite.setAttribute(ad.getName, feature.getAttribute(ad.getName))
      }
      toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(feature.getID)
      toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      fw.write()
      successes +=1
    } catch {
      case e: Exception =>
        logger.error(s"Cannot ingest avro simple feature: $feature, corresponding to line number: $lineNumber", e)
        failures +=1
    }
  }

  def getAttributeIndexInLine(attribute: String) = attributes.indexOf(sft.getDescriptor(attribute))

  def buildIDBuilder: (Seq[String]) => String = {
    (idFields, doHash) match {
       case (s: String, false) =>
         val idSplit = idFields.split(",").map { f => sft.indexOf(f) }
         attrs => idSplit.map { idx => attrs(idx) }.mkString("_")
       case (s: String, true) =>
         val hashFn = Hashing.md5()
         val idSplit = idFields.split(",").map { f => sft.indexOf(f) }
         attrs => hashFn.newHasher().putString(idSplit.map { idx => attrs(idx) }.mkString("_"),
           Charset.defaultCharset()).hash().toString
       case _         =>
         val hashFn = Hashing.md5()
         attrs => hashFn.newHasher().putString(attrs.mkString ("_"),
           Charset.defaultCharset()).hash().toString
     }
  }

  def buildDtBuilder(dtgFieldName: String): Option[(AnyRef) => DateTime] =
    attributes.find(_.getLocalName == dtgFieldName).map {
      case attr if attr.getType.getBinding.equals(classOf[java.lang.Long]) =>
        (obj: AnyRef) => new DateTime(obj.asInstanceOf[java.lang.Long])

      case attr if attr.getType.getBinding.equals(classOf[java.util.Date]) =>
        (obj: AnyRef) => obj match {
          case d: java.util.Date => new DateTime(d)
          case s: String         => dtFormat.parseDateTime(s)
        }

      case attr if attr.getType.getBinding.equals(classOf[java.lang.String]) =>
        (obj: AnyRef) => dtFormat.parseDateTime(obj.asInstanceOf[String])
    }

}

