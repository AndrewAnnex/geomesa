package org.locationtech.geomesa.blob.core.handlers

import java.util
import java.util.{UUID, Date}

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.util.Try
import scala.collection.JavaConversions._

object BlobStoreGenericHandler extends BlobStoreSimpleFeatureBuilder {
  def buildSF(params: util.Map[String, String]): SimpleFeature = {
    val date = getDate(params)
    val geom = getGeometry(params)
    val fileName = getFileName(params)
    buildBlobSF(fileName, geom, date)
  }

  def getDate(params: util.Map[String, String]): Date = getDateFromParams(params).getOrElse(new Date())

  def getDateFromParams(params: util.Map[String, String]): Option[Date] = {
    Try { new Date(params.get(dateFieldName).toLong) }.toOption
  }

  def getGeometry(params: util.Map[String, String]): Geometry = {
    WKTUtils.read(params.getOrElse(geomeFieldName, throw new Exception(s"Could not get Geometry from params $params.")))
  }

  def getFileName(params: util.Map[String, String]): String = {
    params.getOrElse(filenameFieldName, UUID.randomUUID().toString)
  }

}