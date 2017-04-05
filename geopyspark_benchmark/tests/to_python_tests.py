import json
from geopyspark_benchmark.benchmark_utils import setup_environment
setup_environment()

from geopyspark_benchmark.context import Context
from geopyspark.avroserializer import AvroSerializer
from geopyspark.avroregistry import AvroRegistry

import pytest


context = Context(appName="main", master="local[*]")
decoder = AvroRegistry.create_partial_tuple_decoder(None, "Tile")
encoder = AvroRegistry.create_partial_tuple_encoder(None, "Tile")

def benchmark_1():
    result = context.to_python.getHadoopRDD("ProjectedExtent",
                                            "geopyspark_benchmark/tests/data/econic.tif",
                                            context.sc)

    ser = AvroSerializer(result._2(), decoder, encoder)

    ser.loads(result._1()[0])

def benchmark_2():
    result = context.to_python.getHadoopMap("ProjectedExtent",
                                            "geopyspark_benchmark/tests/data/econic.tif",
                                            context.sc)

    projected_extent = {'extent': result['extent'], 'proj4': result['crs']}

    band = {
        'cols': result['cols'],
        'rows': result['rows'],
        'no_data_value': result['noDataValue'],
        'data': result['data']
    }

    multiband = {'bands': [band]}

    [{'_1': projected_extent, '_2': multiband}]

def benchmark_3():
    context.to_python.getHadoopScalaRDD("../econic.tif", context.sc)

def test_apache_avro(benchmark):
    benchmark(benchmark_1)

def test_python_dict(benchmark):
    benchmark(benchmark_2)

def test_scala_rdd(benchmark):
    benchmark(benchmark_3)
