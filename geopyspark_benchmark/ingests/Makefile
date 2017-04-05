python-benchmark:
	PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 spark-submit \
								 --master "local[*]" \
								 --driver-memory 16G \
								 --jars ../geopyspark/geopyspark/jars/geotrellis-backend-assembly-0.1.0.jar \
								 local_ingest.py

remove:
	rm -rf /tmp/python-catalog

run-test: python-benchmark remove
