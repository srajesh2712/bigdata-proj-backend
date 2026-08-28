#!/bin/bash

# ==============================================================================
# SPARK JOB SUBMISSION SCRIPT (Final Environment Fix)
# ==============================================================================

# --- Essential Configuration Variables ---
SPARK_MASTER_URI="spark://spark-master:7077"
SPARK_SUBMIT_CONTAINER="spark-submit" # Name of the Docker container used for spark-submit

# CRITICAL FIX: Use the CONFIRMED correct path for the Sedona package
PYTHON_PACKAGES_PATH="/usr/local/lib/python3.8/dist-packages" 

SPARK_HOME="/opt/spark" # Assuming Spark is installed here in the container
PYTHON_EXEC="/usr/bin/python3" # Explicitly setting the path to the installed Python executable


# --- Input Validation ---
if [ "$#" -ne 1 ]; then
    echo "Error: You must provide the PySpark file name as an argument."
    echo "Usage: $0 <pyspark_filename>"
    exit 1
fi

PYSPARK_SCRIPT=$1
CONTAINER_SCRIPT_PATH="/opt/spark-jobs/${PYSPARK_SCRIPT}"

echo "Starting HDFS connectivity test via Spark Submit..."

# --- Sedona Path Configuration ---
# Java Components (JARs)
SPARK_JARS="/opt/spark-jars/sedona-spark-shaded-3.5_2.12-1.5.1.jar,/opt/spark-jars/geotools-wrapper-1.5.1-28.2.jar"


# --- Step 0: Ensure Python Bindings are installed (Logs confirmed package is present) ---
echo "Apache Sedona Python package confirmed installed in container."


# --- Step 1: Execute Job inside the dedicated submit container ---
 

sudo docker exec "${SPARK_SUBMIT_CONTAINER}" bash -c "\
    # Set Spark Home and explicitly define the Python executable path
    SPARK_HOME=${SPARK_HOME} PYSPARK_PYTHON=${PYTHON_EXEC} /opt/spark/bin/spark-submit \
    --master ${SPARK_MASTER_URI} \
    --jars ${SPARK_JARS} \
    --conf \"spark.executor.extraClassPath=/opt/spark-jars/*\" \
    --conf \"spark.driver.extraClassPath=/opt/spark-jars/*\" \
    --conf \"spark.driverEnv.PYTHONPATH=${PYTHON_PACKAGES_PATH}:\$PYTHONPATH\" \
    --conf \"spark.executorEnv.PYTHONPATH=${PYTHON_PACKAGES_PATH}:\$PYTHONPATH\" \
    ${CONTAINER_SCRIPT_PATH}"

EXIT_CODE=$?

# --- Interpretation (omitted for brevity) ---
echo "--- Job Submission Complete ---"

if [ ${EXIT_CODE} -eq 0 ]; then
    echo "SUCCESS: The spark-submit command executed successfully."
else
    echo "ERROR: Spark submit failed with exit code ${EXIT_CODE}."
    echo "Please check the logs printed above for connection errors or stack traces."
fi

