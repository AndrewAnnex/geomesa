package org.locationtech.geomesa.dynamo.core

import java.nio.ByteBuffer

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.`type`.AttributeDescriptor


trait FeatureSerializer {

}

object FeatureSerializer {

  sealed trait FieldSerializer {
    def serialize(o: java.lang.Object): java.lang.Object
    def deserialize(o: java.lang.Object): java.lang.Object
  }

  case object GeomSerializer extends FieldSerializer {
    override def serialize(o: Object): AnyRef = {
      val geom = o.asInstanceOf[Point]
      ByteBuffer.wrap(WKBUtils.write(geom))
    }

    override def deserialize(o: Object): AnyRef = WKBUtils.read(o.asInstanceOf[ByteBuffer].array())
  }

  case object DefaultSerializer extends FieldSerializer {
    override def serialize(o: Object): AnyRef = o
    override def deserialize(o: Object): AnyRef = o
  }

  object FieldSerializer {
    def apply(attrDescriptor: AttributeDescriptor): FieldSerializer = {
      if (classOf[Geometry].isAssignableFrom(attrDescriptor.getType.getBinding)) GeomSerializer
      else DefaultSerializer
    }
  }

}