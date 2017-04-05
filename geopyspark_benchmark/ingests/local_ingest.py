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
    print('Creating the GeoPyContext\n')
    geopysc = GeoPyContext(appName="python-S3-ingest", master="local[*]")

    #path = "../../Desktop/ingest/data/"

    print('Reading in the RDD\n')
    rdd = geotiff_rdd(geopysc, SPATIAL, "s3://azavea-open-imagery-network/ecuador/pl/1738516_2015-10-24_RE2_3A_373836.tif")
    #rdd = geotiff_rdd(geopysc, SPATIAL, path)#, numPartitions=1000)

    print('Collecting the metadata\n')
    (_, metadata) = collect_pyramid_floating_metadata(geopysc=geopysc,
                                                      rdd_type=SPATIAL,
                                                      raster_rdd=rdd,
                                                      tile_cols=256,
                                                      tile_rows=256)

    print('Tile to layout\n')
    laid_out = tile_to_layout(geopysc, SPATIAL, rdd, metadata)

    (_, reprojected, reprojected_metadata) = reproject_with_zoomed_layout(geopysc,
                                                                          SPATIAL,
                                                                          laid_out,
                                                                          metadata,
                                                                          "EPSG:3857",
                                                                          tile_size=256)


    print('Pyramiding\n')
    pyramided = pyramid(geopysc,
                        SPATIAL,
                        reprojected,
                        reprojected_metadata,
                        256,
                        12,
                        0)

    print('Writing\n')
    for zoom, layer_rdd, layer_metadata in pyramided:
        write(geopysc,
              SPATIAL,
              "file:///tmp/python-catalog",
              "python-benchmark",
              zoom,
              layer_rdd,
              layer_metadata)
