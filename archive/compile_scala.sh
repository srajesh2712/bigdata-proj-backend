#!/bin/bash

# --- REVERTING TO HOST-SIDE COMPILATION ---
# The container environment is too restricted to locate 'scalac'.
# We will compile on the local Ubuntu host, which requires copying dependencies 
# from the container first.

SCALA_FILE="CustomSedonaIO.scala"
JAR_NAME="custom_sedona_io.jar"
CONTAINER_NAME="spark-submit"
CONTAINER_SCRIPT_PATH="/opt/spark-jobs"
TEMP_DEP_DIR="/tmp/sedona_deps_$$" # Unique temp dir on host for safety

echo "--- Custom Sedona I/O Compilation (Back to HOST system) ---"

# --- Step 0: Setup and Dependency Copy ---
echo "1. Creating temporary dependency directory on host: ${TEMP_DEP_DIR}"
mkdir -p ${TEMP_DEP_DIR}

echo "2. Copying necessary Spark/Sedona dependency JARs from container to host (approx 338MB)..."
# Copy all Spark/Sedona jars from the container's classpath to the host's temp directory
sudo docker cp ${CONTAINER_NAME}:/opt/spark/jars/. ${TEMP_DEP_DIR}
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy JARs from container. Check container name and permissions."
    rm -rf ${TEMP_DEP_DIR}
    exit 1
fi

# --- Step 1: Compile the Scala class (on the Host) ---
echo "3. Compiling ${SCALA_FILE} on the HOST system (requires local 'scalac')."

# Create the destination directory before compiling
mkdir -p compiled_classes

echo "Generating robust classpath string..."
# Find all JARs, print them line-by-line, replace newlines with colons, and trim the trailing colon.
# This is the most robust way to build the classpath list in a shell script.
SCALA_CLASSPATH=$(find "${TEMP_DEP_DIR}" -name "*.jar" | tr '\n' ':' | sed 's/:$//')

# Execute scalac on the host system, including the current directory (.) and the built classpath.
# We explicitly call CustomSedonaIO.scala
scalac -d compiled_classes -cp "${SCALA_CLASSPATH}" ${SCALA_FILE}
if [ $? -ne 0 ]; then
    echo "ERROR: Scala compilation failed. The compiler could not find necessary packages (e.g., org.apache.spark)."
    echo "This is usually due to a classpath size limit or an incompatibility between local scalac (2.11.12) and the Spark/Sedona JARs."
    rm -rf compiled_classes ${TEMP_DEP_DIR}
    exit 1
fi

# --- Step 2: Package the compiled classes into the final JAR (on the Host) ---
echo "4. Packaging classes into the final JAR: ${JAR_NAME} (requires local 'jar')."
# Execute jar command on the host system
jar cvf ${JAR_NAME} -C compiled_classes .
if [ $? -ne 0 ]; then
    echo "ERROR: JAR packaging failed. Check if 'jar' (part of the JDK) is installed on your host."
    rm -rf compiled_classes ${TEMP_DEP_DIR}
    exit 1
fi

# --- Step 3: Copy the final JAR back into the container ---
echo "5. Copying the final JAR back into the container at ${CONTAINER_SCRIPT_PATH}/${JAR_NAME}..."
sudo docker cp ${JAR_NAME} ${CONTAINER_NAME}:${CONTAINER_SCRIPT_PATH}/${JAR_NAME}

# --- Step 4: Cleanup ---
echo "6. Cleaning up temporary files on host..."
rm -rf compiled_classes ${TEMP_DEP_DIR}

echo "--- Compilation and Deployment Complete ---"
echo "The JAR file, ${JAR_NAME}, is now deployed inside the container at ${CONTAINER_SCRIPT_PATH}/${JAR_NAME}."
echo "You MUST now update your job_submission.sh script to include this JAR in the '--jars' list."
echo "------------------------------------------------------------------------"

