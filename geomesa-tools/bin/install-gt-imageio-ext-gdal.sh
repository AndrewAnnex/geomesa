#! /usr/bin/env bash
#
# Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#

# Check environment variables before running anything, warn user on issues:
if [[ (-z "$GEOMESA_HOME") ]]; then
    echo "Error: GEOMESA_HOME environmental variable not found...install geomesa or define GEOMESA_HOME and try again"
    exit
else
    osgeo_url='http://download.osgeo.org'
    mvn_url='http://central.maven.org'

    url_gtimageioextgdal="${osgeo_url}/webdav/geotools/org/geotools/gt-imageio-ext-gdal/14.1/gt-imageio-ext-gdal-14.1.jar"

    read -r -p "GeoTools-imageio-ext-gdal is GNU Lesser General Public licensed and is not distributed with GeoMesa...are you sure you want to install it from $url_gtimageioextgdal ? [Y/n]" confirm
    confirm=${confirm,,} #lowercasing
    if [[ $confirm =~ ^(yes|y) ]]; then
        echo "Trying to install GeoTools-imageio-ext-gdal from $url_gtimageioextgdal to $GEOMESA_HOME"
        wget -O $GEOMESA_HOME/lib/gt-imageio-ext-gdal-14.1.jar $url_gtimageioextgdal \
            && chmod 0755 $GEOMESA_HOME/lib/gt-imageio-ext-gdal-14.1.jar \
            && echo "Successfully installed gt-imageio-ext-gdal to $GEOMESA_HOME"
    else
        echo "Cancelled installation of gt-imageio-ext-gdal 14.1"
    fi
fi
