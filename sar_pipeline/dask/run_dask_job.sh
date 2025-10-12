#!/bin/bash

# --- Configuration ---
# The name of the Dask Client service in your docker-compose.yml
DASK_CLIENT_SERVICE="dask-client"

# The name of your Python file to execute
PYTHON_FILE="dask_flood_mask_workaround.py"

# The path to the Python file inside the container
# This must match the volume mount: /opt/spark-jobs
INTERNAL_PATH="/opt/spark-jobs/${PYTHON_FILE}"

# --- Execution ---

echo "Starting Dask job submission..."
echo "Running ${PYTHON_FILE} inside the ${DASK_CLIENT_SERVICE} container."

# Use 'docker compose exec' to run the Python script inside the dask-client container.
# The script will automatically connect to 'dask-scheduler:8786' as defined in the
# DASK_SCHEDULER environment variable or within the Python file itself.
docker compose exec -it ${DASK_CLIENT_SERVICE} python3 ${INTERNAL_PATH}

# Check the exit status of the python command
if [ $? -eq 0 ]; then
  echo "✅ Dask job completed successfully."
else
  echo "❌ Dask job failed. Check the logs and the dask-client container."
fi
