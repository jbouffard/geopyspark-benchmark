from pyspark import SparkContext
from geopyspark.geopycontext import GeoPyContext


class Context(GeoPyContext):
    def __init__(self, pysc=None, **kwargs):
        super().__init__(pysc, **kwargs)

    @property
    def to_python(self):
        return self._jvm.geopyspark.benchmark.BenchmarkToPython

    @property
    def from_python(self):
        return self._jvm.geopyspark.benchmark.BenchmarkFromPython
