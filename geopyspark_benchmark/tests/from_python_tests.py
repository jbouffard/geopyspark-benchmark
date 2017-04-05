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

result = context.from_python.getHadoopRDD("geopyspark_benchmark/tests/data/econic.tif", context.sc)

ser = context.create_tuple_serializer(result._2(), value_type="Tile")
rdd_1 = context.create_python_rdd(result._1(), ser)

rdd_2 = context.to_python.getHadoopScalaRDD("geopyspark_benchmark/tests/data/econic.tif", context.sc)


def benchmark_1():
    schema = rdd_1._jrdd_deserializer.serializer.schema_string
    context.from_python.takePythonRDD(rdd_1._jrdd, schema)

def benchmark_2():
    context.from_python.takeScalaRDD(rdd_2)

def test_python_rdd_to_scala(benchmark):
    benchmark(benchmark_1)

def test_scala_rdd_to_scala(benchmark):
    benchmark(benchmark_2)
