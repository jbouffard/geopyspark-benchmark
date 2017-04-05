This is a simple benchmarking tool for GeoPySpark.

pytest-benchmark, a pytest fixture used to benchmark code, is the benchmarking utility used
in this project. In order to run the benchmarks, go to the root directory of this project and
run the following:

```text
pytest geopyspark_benchmark/tests/name_of_test.py \
--benchmark-warmup=on \
--benchmark-min-rounds=100
```

Where `name_of_test.py` is the name of the actual file to run. Unfortunately, it is not possible to run
all benchmarks at once at this time. If you wish to save your results, then add the following line
to the bottom of the previous mentioned command:

```text
--benchmark-save=NAME
```

Where `NAME` is the name of the file to save the results to. All results are saved in the .benchmarks/
directory found within the root of the project.
