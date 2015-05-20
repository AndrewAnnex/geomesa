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

// This is the RasterIndexSchema, for more details see IndexSchema and SchemaHelpers

package org.locationtech.geomesa.raster.index

import org.locationtech.geomesa.accumulo.index.{CompositeTextFormatter, KeyValuePair, PartitionTextFormatter}
import org.locationtech.geomesa.raster.data.Raster

case class RasterIndexSchema(encoder: RasterIndexEntryEncoder,
                             decoder: RasterIndexEntryDecoder) {

  def encode(raster: Raster, visibility: String = "") = encoder.encode(raster, visibility)
  def decode(entry: KeyValuePair): Raster = decoder.decode(entry)

  // utility method to ask for the maximum allowable shard number
  def maxShard: Int =
    encoder.rowf match {
      case CompositeTextFormatter(Seq(PartitionTextFormatter(numPartitions), xs@_*), sep) => numPartitions
      case _ => 1  // couldn't find a matching partitioner
    }

}

object RasterIndexSchema {

  def apply(): RasterIndexSchema = {
    val keyEncoder = RasterIndexEntryEncoder(null, null, null)
    val indexEntryDecoder = RasterIndexEntryDecoder()
    RasterIndexSchema(keyEncoder, indexEntryDecoder)
  }

}