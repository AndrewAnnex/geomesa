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

package org.locationtech.geomesa.raster.feature

import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.factory.{AbstractFactory, Hints}
import org.geotools.referencing.crs.DefaultGeographicCRS

class GeomesaRasterFeatureFactory extends AbstractFactory {

}

object GeomesaRasterFeatureFactory {

  def init = {
    Hints.putSystemDefault(Hints.GRID_COVERAGE_FACTORY, classOf[GeomesaRasterFeatureFactory])
    Hints.putSystemDefault(Hints.TILE_ENCODING, "raw")
    Hints.putSystemDefault(Hints.DEFAULT_COORDINATE_REFERENCE_SYSTEM, DefaultGeographicCRS.WGS84)
  }

  private val hints = new Hints(Hints.GRID_COVERAGE_FACTORY, classOf[GeomesaRasterFeatureFactory])
  private val gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(hints)


}
