package org.locationtech.geomesa.convert

import java.io.InputStream

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class SchemaToSchemaConverterFactory extends SimpleFeatureConverterFactory[SimpleFeature] {
  override def canProcess(conf: Config): Boolean = canProcessType(conf, "simplefeature")

  override def buildConverter(targetSft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[SimpleFeature] = {
    // TODO: have mapings here for avro to avro, geojson to geojson, avro to geojson? etc?
    val inputSFTName = conf.getString("input-sft")
    val inputSFT = SimpleFeatureTypeLoader.sftForName(inputSFTName)
      .getOrElse(throw new IllegalArgumentException(s"Unable to load SFT for typeName $inputSFTName"))
    val fields = buildFields(conf.getConfigList("fields"))
    val idBuilder = buildIdBuilder(conf.getString("id-field"))
    val validating = isValidating(conf)

    new SchemaToSchemaConverter(inputSFT, targetSft, fields, idBuilder, validating)
  }
}


class SchemaToSchemaConverter(val inputSFT: SimpleFeatureType,
                               val targetSFT: SimpleFeatureType,
                               val inputFields: IndexedSeq[Field],
                               val idBuilder: Expr,
                               val validating: Boolean)
  extends ToSimpleFeatureConverter[SimpleFeature] {

  override def fromInputType(i: SimpleFeature): Seq[Array[Any]] = Seq(i.getAttributes.toArray[Any])

  override def process(is: InputStream, ec: EvaluationContext): Iterator[SimpleFeature] = ???
}
