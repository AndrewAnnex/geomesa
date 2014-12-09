package org.locationtech.geomesa.raster.data

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geohash.BoundingBox

// TODO: Constructor needs info to create Row Formatter
case class AccumuloRasterQueryPlanner(schema: String) extends Logging {

  def getQueryPlan(rq: RasterQuery): QueryPlan = {
    val hashes = BoundingBox.getGeoHashesFromBoundingBox(rq.bbox)
    val res = lexiEncodeDoubleToString(rq.resolution)
    logger.debug(s"Planner: BBox: ${rq.bbox} has geohashes: $hashes , and has encoded Resolution: $res")

    val rows = hashes.map { gh =>
      // TODO: Use Row Formatter here
      // GEOMESA-555
      //val schemaStr = s"%~#s%$res#ires~$gh"
      new org.apache.accumulo.core.data.Range(new Text(s"$res~$gh"))
    }

    // TODO: Configure Iterators and any ColumnFamilies
    QueryPlan(Seq(), rows, Seq())
  }
}
