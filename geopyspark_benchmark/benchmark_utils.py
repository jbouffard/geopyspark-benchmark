import glob
import sys

import os
from os import path


VERSION = '0.1.0'
JAR_FILE = 'geopyspark-backend-assembly-' + VERSION + '.jar'


def add_pyspark_path():
    try:
        pyspark_home = os.environ["SPARK_HOME"]
        sys.path.append(path.join(pyspark_home, 'python'))

    except:
        raise KeyError("Could not find SPARK_HOME")

def setup_environment():
    add_pyspark_path()

    current_location = path.dirname(path.realpath(__file__))

    local_prefixes = [
        path.abspath(path.join(current_location, 'jars/')),
        path.abspath(path.join(os.getcwd(), 'jars/'))
    ]

    possible_jars = [path.join(prefix, '*.jar') for prefix in local_prefixes]

    returned = [glob.glob(jar_files) for jar_files in possible_jars]
    jars = [jar for sublist in returned for jar in sublist]

    if len(jars) == 0:
        raise IOError("Failed to find any jars. Looked at these paths {}".format(possible_jars))

    jar_string = ','.join(jars)

    os.environ['JARS'] = jar_string
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars {} \
            --conf spark.ui.enabled=false \
            --driver-memory 8G \
            --executor-memory 8G \
            pyspark-shell".format(jar_string)
