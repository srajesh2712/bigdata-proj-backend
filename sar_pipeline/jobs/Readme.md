# SAR Data Preparation and Distributed Processing

This document describes the procedure for downloading the Sentinel-1 SAR data, configuring the Docker environment, starting the required services, and executing the **PySNAP, Apache Spark, and Dask** preprocessing and processing workflows used in this dissertation.

---

## 1. Data Preparation

### 1.1 Data Source

The Sentinel-1 SAR data used in this study were obtained from the **Alaska Satellite Facility (ASF) Data Search**.

Register for a free account and log in:

https://search.asf.alaska.edu

The following Sentinel-1 GRD High Definition (GRD_HD) products are required. Each compressed product is approximately **900 MB or larger**.

### 1.2 Sentinel-1 Scenes

| Scene | Product |
|---:|---|
| 4 | `S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE` |
| 6 | `S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.SAFE` |
| 5 | `S1A_IW_GRDH_1SDV_20260208T063005_20260208T063030_063124_07EC55_119E.SAFE` |
| 7 | `S1A_IW_GRDH_1SDV_20260220T063005_20260220T063030_063299_07F2DE_E43B.SAFE` |

### 1.3 Download Links

The required products can be downloaded using the following ASF Data Pool links:

**Scene 4**

https://datapool.asf.alaska.edu/GRD_HD/SA/S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.zip

**Scene 6**

https://datapool.asf.alaska.edu/GRD_HD/SA/S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.zip

**Scene 5**

https://datapool.asf.alaska.edu/GRD_HD/SA/S1A_IW_GRDH_1SDV_20260208T063005_20260208T063030_063124_07EC55_119E.zip

**Scene 7**

https://datapool.asf.alaska.edu/GRD_HD/SA/S1A_IW_GRDH_1SDV_20260220T063005_20260220T063030_063299_07F2DE_E43B.zip

Download all four ZIP files and extract them into the local SAR data directory.

For example:

```text
SAR_Data/
├── S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE/
├── S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.SAFE/
├── S1A_IW_GRDH_1SDV_20260208T063005_20260208T063030_063124_07EC55_119E.SAFE/
└── S1A_IW_GRDH_1SDV_20260220T063005_20260220T063030_063299_07F2DE_E43B.SAFE/
```



# 2. Prerequisites and External Dependencies

Before starting the Docker services, the following external dependencies must be installed and made available to the Spark and Dask containers:

- ESA SNAP and Sentinel-1 Toolbox
- Hadoop Native Libraries

These dependencies are mounted into the Docker containers through docker-compose.yml.


## 2.1 ESA SNAP

ESA SNAP is required for Sentinel-1 SAR preprocessing using the SNAP processing engine.

Download SNAP from the official ESA Science Toolbox Exploitation Platform (STEP):

https://step.esa.int/main/download/snap-download/

The download page provides installers for Linux 64-bit, Windows, and macOS. For this project, use the Linux 64-bit installer and ensure that the Sentinel Toolboxes, including the Sentinel-1 Toolbox, are installed.

Installation

Download and install SNAP on the host machine.
Installation instruction provided in the above link. Refer for the OS in which you install. 

After installation, the directory should contain the SNAP installation files, including the bin, etc, and other SNAP directories.

Example:

esa-snap/
Mount SNAP into Docker

The SNAP installation must be mounted into the Spark and Dask containers.

Add the following volume mapping to the required services in docker-compose.yml:

- /home/<USER>/esa-snap:/opt/snap


Important: The same SNAP installation should be mounted into every container that executes SNAP processing. This includes the Spark master/worker containers and the Dask client/worker containers used by the processing workflow.


## 2.2 Hadoop Native Libraries

The Spark and Dask processing workflows communicate with Hadoop/HDFS. The Hadoop native libraries are therefore required for the Hadoop runtime environment.

On Linux, the Hadoop native library includes the dynamically linked library:

libhadoop.so

Hadoop provides pre-built native libraries as part of Hadoop distributions, or they can be built from source.

### Obtain Hadoop Native Libraries

Use the Hadoop distribution corresponding to the Hadoop version used by this project.

https://hadoop.apache.org/releases.html

After extracting Hadoop, locate the native libraries directory.

A typical Hadoop installation contains:

hadoop/
├── bin/
├── etc/
├── lib/
├── libexec/
├── sbin/
└── share/

The native libraries are commonly located under:

$HADOOP_HOME/lib/native/

Verify that the directory contains the native Hadoop library:

ls -l $HADOOP_HOME/lib/native/

The following file should be available:

libhadoop.so
Place Hadoop Native Files in the Spark / Dask folder 

### Mount Hadoop Native Libraries into Docker

This is done in the Dockerfile.sar where the native libraries are copied to the docker image 

Jars for Spark - 
Download these jars and place it in jars folder inside apache-spark 
geotools-wrapper-1.8.1-33.1.jar - https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper
postgresql-42.6.0.jar - https://mvnrepository.com/artifact/org.postgresql/postgresql



---

# 2. Docker Configuration

The processing environment is containerised using Docker. The host directories containing the SAR data, SNAP installation, and processing scripts must be mounted into the appropriate containers.

Before starting the services, update the volume paths in `docker-compose.yml`.

## 2.1 SNAP Installation

Mount the local ESA SNAP installation into the Docker containers:

```yaml
- /home/<USER>/esa-snap:/opt/snap
```

The host path (`/home/<USER>/esa-snap`) should be replaced with the location of the SNAP installation on the machine.

Inside the container, SNAP is available at:

```text
/opt/snap
```

## 2.2 SAR Data

Mount the directory containing the downloaded SAR products:

```yaml
- /home/<USER>/Documents/SAR_Data:/opt/spark/data
```

The host directory is:

```text
/home/<USER>/Documents/SAR_Data
```

and it is exposed inside the container as:

```text
/opt/spark/data
```

## 2.3 Processing Jobs

Mount the project `jobs` directory:

```yaml
- /home/<USER>/Documents/summer-project-codes/bigdata-proj-backend/sar_pipeline/jobs:/opt/spark-jobs
```

The processing scripts are therefore available inside the containers under:

```text
/opt/spark-jobs
```

## 2.4 Required Containers

The above volume mounts should be configured for the following services in `docker-compose.yml`:

```text
spark-worker-1
spark-master
spark-worker-2
spark-submit
```

The exact configuration should be retained consistently across these services so that the required data, SNAP installation, and processing scripts are accessible to the distributed workers.

> **Important:** In a Docker volume mapping, the path before the colon (`:`) is the path on the host machine, while the path after the colon is the path inside the Docker container.

For example:

```yaml
- /home/<USER>/Documents/SAR_Data:/opt/spark/data
```

means:

```text
Host:      /home/<USER>/Documents/SAR_Data
Container: /opt/spark/data
```

---

# 3. Software Environment

The software environment required for the dissertation experiments is provided using Docker.

The main distributed processing frameworks used are:

- **Apache Hadoop**
- **Apache Spark**
- **Dask**
- **PostgreSQL**
- **ESA SNAP**

The services can be started using the provided `start.sh` script.

## 3.1 Start Hadoop

```bash
./start.sh hadoop up
```

## 3.2 Start Spark

```bash
./start.sh spark up
```

## 3.3 Start Dask

```bash
./start.sh dask up
```

## 3.4 Start PostgreSQL

```bash
./start.sh postgres up
```

After starting the services, verify that the corresponding web interfaces are accessible.

| Service | Web Interface |
|---|---|
| Spark | http://localhost:9090/ |
| Hadoop | http://localhost:9870/ |
| Dask | http://localhost:8787/status |

---

# 4. SAR Preprocessing

Three preprocessing approaches are used in the experimental workflow:

1. PySNAP
2. Spark
3. Dask

The preprocessing scripts operate on the Sentinel-1 SAR products and generate the preprocessed data required for subsequent processing.

## 4.1 PySNAP Preprocessing

The PySNAP workflow can be executed using:

```bash
python main.py
```

This executes the SNAP-based preprocessing workflow.

## 4.2 Spark-Based Preprocessing

The Spark preprocessing workflow is executed through the `spark-submit` container.

```bash
docker exec -it spark-submit bash -c "/opt/spark/bin/spark-submit \
  --jars /opt/spark-jars/postgresql-42.6.0.jar \
  --master spark://spark-master:7077 \
  --driver-memory 1G \
  --executor-memory 6G \
  --executor-cores 2 \
  /opt/spark-jobs/preprocessing/preprocess_spark_db.py"
```

The Spark application connects to the Spark cluster and distributes preprocessing tasks across the available Spark workers.

## 4.3 Dask-Based Preprocessing

The Dask preprocessing workflow is executed using the Dask client:

```bash
docker exec -it dask-client \
  python /opt/spark-jobs/preprocessing/preprocess_dask_db.py
```

## 4.4 GeoTIFF to Zarr Conversion

The preprocessing workflow can also convert GeoTIFF data into the Zarr format using:

```bash
docker exec -it dask-client \
  python /opt/spark-jobs/processing/zarr_tif.py
```

> **Note:** Ensure that the filename matches the actual script in the `jobs/processing` directory. If the script is named `zar_tif.py` in the project, use that filename instead.

---

# 5. Downloading Preprocessed Data from Hadoop

Preprocessed files stored in HDFS can be copied back to the local machine using the Hadoop filesystem command.

For example:

```bash
hdfs get hdfs://localhost:8020/user/<USER>/spark_preprocessed/8/40_tile.tif \
  40_tile_spark_preprocessed.tif
```

This downloads the specified GeoTIFF from HDFS to the current local directory.

---

# 6. SAR Processing

Following preprocessing, the generated data can be processed using Spark or Dask.

Two processing formats are evaluated:

- GeoTIFF
- Zarr

This allows the performance of the different distributed processing approaches and data formats to be compared.

## 6.1 Spark GeoTIFF Processing

```bash
docker exec -it spark-submit bash -c "/opt/spark/bin/spark-submit \
  --jars /opt/spark-jars/postgresql-42.6.0.jar \
  --master spark://spark-master:7077 \
  --driver-memory 1G \
  --executor-memory 6G \
  --executor-cores 2 \
  /opt/spark-jobs/processing/spark_geotiff_processing.py"
```

## 6.2 Spark Zarr Processing

```bash
docker exec -it spark-submit bash -c "/opt/spark/bin/spark-submit \
  --jars /opt/spark-jars/postgresql-42.6.0.jar \
  --master spark://spark-master:7077 \
  --driver-memory 1G \
  --executor-memory 6G \
  --executor-cores 2 \
  /opt/spark-jobs/processing/spark_zarr_processing.py"
```

## 6.3 Dask GeoTIFF Processing

```bash
docker exec -it dask-client \
  python /opt/spark-jobs/processing/dask_geotiff_processing.py
```

## 6.4 Dask Zarr Processing

```bash
docker exec -it dask-client \
  python /opt/spark-jobs/processing/dask_zarr_processing.py
```

---

# 7. Processing Workflow

The complete experimental workflow can be summarised as:

```text
Sentinel-1 SAR Products
          │
          ▼
     ASF Data Pool
          │
          ▼
      Local Storage
          │
          ▼
     Docker Containers
          │
          ├───────────────┐
          ▼               ▼
       PySNAP          Distributed
                       Processing
                       ┌──────┴──────┐
                       ▼             ▼
                     Spark         Dask
                       │             │
                       └──────┬──────┘
                              ▼
                       Preprocessed Data
                              │
                     ┌────────┴────────┐
                     ▼                 ▼
                  GeoTIFF             Zarr
                     │                 │
                     └────────┬────────┘
                              ▼
                         Processing
                              │
                              ▼
                     Performance Results
```

The workflows allow the preprocessing and processing performance of **PySNAP, Spark, and Dask**, together with **GeoTIFF and Zarr** data representations, to be evaluated under the experimental configuration used in this dissertation.

---

# 8. Debugging and Verification

If a processing container is running but the expected scripts are not available, access the Dask client container:

```bash
docker exec -it dask-client bash
```

Then list the mounted processing directory:

```bash
ls -l /opt/spark-jobs
```

The expected directory structure should include:

```text
/opt/spark-jobs/
├── preprocessing/
└── processing/
```

The processing scripts can then be inspected using:

```bash
ls -l /opt/spark-jobs/preprocessing
ls -l /opt/spark-jobs/processing
```

If a script cannot be found, first verify that the host-side `jobs` directory is correctly mounted in `docker-compose.yml`.

---

# 9. Summary of Commands

For convenience, the main commands used in the experimental workflow are listed below.

### Start Services

```bash
./start.sh hadoop up
./start.sh spark up
./start.sh dask up
./start.sh postgres up
```

### PySNAP Preprocessing

```bash
python main.py
```

### Spark Preprocessing

```bash
docker exec -it spark-submit bash -c "/opt/spark/bin/spark-submit \
  --jars /opt/spark-jars/postgresql-42.6.0.jar \
  --master spark://spark-master:7077 \
  --driver-memory 1G \
  --executor-memory 6G \
  --executor-cores 2 \
  /opt/spark-jobs/preprocessing/preprocess_spark_db.py"
```

### Dask Preprocessing

```bash
docker exec -it dask-client \
  python /opt/spark-jobs/preprocessing/preprocess_dask_db.py
```

### Spark GeoTIFF Processing

```bash
docker exec -it spark-submit bash -c "/opt/spark/bin/spark-submit \
  --jars /opt/spark-jars/postgresql-42.6.0.jar \
  --master spark://spark-master:7077 \
  --driver-memory 1G \
  --executor-memory 6G \
  --executor-cores 2 \
  /opt/spark-jobs/processing/spark_geotiff_processing.py"
```

### Spark Zarr Processing

```bash
docker exec -it spark-submit bash -c "/opt/spark/bin/spark-submit \
  --jars /opt/spark-jars/postgresql-42.6.0.jar \
  --master spark://spark-master:7077 \
  --driver-memory 1G \
  --executor-memory 6G \
  --executor-cores 2 \
  /opt/spark-jobs/processing/spark_zarr_processing.py"
```

### Dask GeoTIFF Processing

```bash
docker exec -it dask-client \
  python /opt/spark-jobs/processing/dask_geotiff_processing.py
```

### Dask Zarr Processing

```bash
docker exec -it dask-client \
  python /opt/spark-jobs/processing/dask_zarr_processing.py
```

---

# 10. Reproducibility

To reproduce the experiments, the following components are required:

1. The four Sentinel-1 SAR GRD products listed in Section 1.
2. The project source code and `jobs` directory.
3. The Docker and Docker Compose configuration.
4. The ESA SNAP installation.
5. The required Docker volume mounts.
6. The Hadoop, Spark, Dask, and PostgreSQL services.
7. Execution of the preprocessing and processing commands described above.

The combination of the supplied SAR products, Docker configuration, source code, and commands provides the environment required to reproduce the experimental workflows presented in this dissertation.



