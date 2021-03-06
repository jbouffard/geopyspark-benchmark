import rasterio

from geopyspark.geopycontext import GeoPyContext
from geopyspark.geotrellis.constants import SPATIAL
from geopyspark.geotrellis.geotiff_rdd import geotiff_rdd
from geopyspark.geotrellis.tile_layer import (collect_pyramid_zoomed_metadata,
                                              collect_pyramid_floating_metadata,
                                              tile_to_layout,
                                              pyramid,
                                              reproject_with_zoomed_layout)
from geopyspark.geotrellis.catalog import write, read


if __name__ == "__main__":
    print('Creating the GeoPyContext')
    geopysc = GeoPyContext(appName="sentinel-ingest", master="local[*]")

    path = "../../Desktop/18TVK-2016-10-18-0-60m.tif"

    print("Reading in RDD")
    with rasterio.open(path) as f:
        tile = f.read()
        no_data = f.nodata

        bounds = f.bounds

    geopy_tile = {
        'data': tile,
        'no_data_value': no_data
    }

    geopy_extent = {
        'xmin': bounds.left,
        'ymin': bounds.bottom,
        'xmax': bounds.right,
        'ymax': bounds.top
    }

    projected_extent = {'epsg': 32618, 'extent': geopy_extent}

    rdd = geopysc.pysc.parallelize([(projected_extent, geopy_tile)])

    (_, metadata) = collect_pyramid_floating_metadata(geopysc=geopysc,
                                                      rdd_type=SPATIAL,
                                                      raster_rdd=rdd,
                                                      tile_cols=256,
                                                      tile_rows=256)

    print('Tile to layout')
    laid_out = tile_to_layout(geopysc, SPATIAL, rdd, metadata)

    (_, reprojected, reprojected_metadata) = reproject_with_zoomed_layout(geopysc,
                                                                          SPATIAL,
                                                                          laid_out,
                                                                          metadata,
                                                                          "EPSG:3857",
                                                                          tile_size=256)


    print('Pyramiding')
    pyramided = pyramid(geopysc,
                        SPATIAL,
                        reprojected,
                        reprojected_metadata,
                        256,
                        12,
                        0)

    print('Writing')
    for zoom, layer_rdd, layer_metadata in pyramided:
        write(geopysc,
              SPATIAL,
              "file:///tmp/python-catalog",
              "python-benchmark",
              zoom,
              layer_rdd,
              layer_metadata)
