
CREATE SCHEMA IF NOT EXISTS sar;
 
CREATE TABLE sar.sar_scene_master (
    scene_id            BIGSERIAL PRIMARY KEY,

    mission             VARCHAR(50) NOT NULL DEFAULT 'Sentinel-1',
    satellite           VARCHAR(10) NOT NULL,         -- S1A / S1B
    product_type        VARCHAR(50) NOT NULL,         -- GRD / SLC
    mode                VARCHAR(50),                  -- IW / EW / SM
    polarization        VARCHAR(20),                  -- VV,VH / HH,HV
    orbit_direction     VARCHAR(10),                  -- ASC / DESC
    relative_orbit      INT,
    absolute_orbit      INT,

    acquisition_start   TIMESTAMP NOT NULL,
    acquisition_stop    TIMESTAMP NOT NULL,

    scene_name          TEXT NOT NULL UNIQUE,         -- SAFE name
    download_source     TEXT,                         -- ASF, SciHub, Copernicus
    download_time       TIMESTAMP DEFAULT NOW(),

    local_path          TEXT,
    hdfs_raw_path       TEXT,
    file_size_bytes     BIGINT,

    checksum_sha256     TEXT,                         -- reproducibility
    status              VARCHAR(20) NOT NULL DEFAULT 'DOWNLOADED',

    footprint_wkt       TEXT,
    center_lat          DOUBLE PRECISION,
    center_lon          DOUBLE PRECISION,

    metadata_json       JSONB,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_scene_master_time ON sar.sar_scene_master(acquisition_start);
CREATE INDEX idx_scene_master_satellite ON sar.sar_scene_master(satellite);
CREATE INDEX idx_scene_master_product ON sar.sar_scene_master(product_type);



CREATE TABLE sar.processing_job (
    job_id              BIGSERIAL PRIMARY KEY,

    job_name            TEXT NOT NULL,
    job_description     TEXT,

    created_by          TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),

    engine              VARCHAR(20) NOT NULL, -- SPARK / DASK / STANDALONE
    pipeline_type       VARCHAR(50) NOT NULL, -- PREPROCESS / FLOODMASK / INSAR
    stage_name          VARCHAR(50),

    job_status          VARCHAR(20) NOT NULL DEFAULT 'CREATED',

    priority            INT DEFAULT 5,

    parameters_json     JSONB NOT NULL,
    notes               TEXT,

    git_commit_hash     TEXT,
    docker_image        TEXT,
    code_version        TEXT,

    start_time          TIMESTAMP,
    end_time            TIMESTAMP,

    total_tasks         INT DEFAULT 0,
    completed_tasks     INT DEFAULT 0,
    failed_tasks        INT DEFAULT 0,

    created_host        TEXT
);

CREATE INDEX idx_job_status ON sar.processing_job(job_status);
CREATE INDEX idx_job_engine ON sar.processing_job(engine);
CREATE INDEX idx_job_created_at ON sar.processing_job(created_at);



CREATE TABLE sar.job_input_scenes (
    job_id      BIGINT REFERENCES sar.processing_job(job_id) ON DELETE CASCADE,
    scene_id    BIGINT REFERENCES sar.sar_scene_master(scene_id) ON DELETE RESTRICT,

    role        VARCHAR(20) DEFAULT 'PRIMARY',

    PRIMARY KEY (job_id, scene_id)
);

CREATE INDEX idx_job_input_scene ON sar.job_input_scenes(scene_id);


CREATE TABLE sar.job_regions (
    region_id       BIGSERIAL PRIMARY KEY,
    job_id          BIGINT REFERENCES sar.processing_job(job_id) ON DELETE CASCADE,

    region_name     TEXT NOT NULL,
    region_wkt      TEXT NOT NULL,

    bbox_minx       DOUBLE PRECISION,
    bbox_miny       DOUBLE PRECISION,
    bbox_maxx       DOUBLE PRECISION,
    bbox_maxy       DOUBLE PRECISION,

    crs_epsg        INT DEFAULT 4326,

    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_regions_job ON sar.job_regions(job_id);



CREATE TABLE sar.job_tasks (
    task_id         BIGSERIAL PRIMARY KEY,

    job_id          BIGINT REFERENCES sar.processing_job(job_id) ON DELETE CASCADE,
    region_id       BIGINT REFERENCES sar.job_regions(region_id) ON DELETE CASCADE,

    task_name       TEXT,
    tile_index      INT,
    tile_wkt        TEXT,

    task_status     VARCHAR(20) NOT NULL DEFAULT 'CREATED',

    attempt_no      INT DEFAULT 0,
    max_retries     INT DEFAULT 3,

    assigned_worker TEXT,
    assigned_time   TIMESTAMP,

    start_time      TIMESTAMP,
    end_time        TIMESTAMP,

    error_message   TEXT,
    logs_path       TEXT,

    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_task_status ON sar.job_tasks(task_status);
CREATE INDEX idx_task_job ON sar.job_tasks(job_id);
CREATE INDEX idx_task_region ON sar.job_tasks(region_id);

 CREATE TABLE sar.task_metrics (
    metric_id           BIGSERIAL PRIMARY KEY,
    task_id             BIGINT REFERENCES sar.job_tasks(task_id) ON DELETE CASCADE,

    engine              VARCHAR(20),

    driver_memory_mb    INT,
    executor_memory_mb  INT,
    executor_cores      INT,
    num_executors       INT,

    worker_ram_total_mb INT,
    worker_ram_used_mb  INT,
    worker_cpu_cores    INT,

    io_read_mb          DOUBLE PRECISION,
    io_write_mb         DOUBLE PRECISION,
    hdfs_read_mb        DOUBLE PRECISION,
    hdfs_write_mb       DOUBLE PRECISION,

    processing_seconds  DOUBLE PRECISION,
    wall_clock_seconds  DOUBLE PRECISION,

    start_time          TIMESTAMP,
    end_time            TIMESTAMP,

    cpu_util_avg        DOUBLE PRECISION,
    mem_util_avg        DOUBLE PRECISION,

    extra_metrics_json  JSONB,

    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_metrics_task ON sar.task_metrics(task_id);




CREATE TABLE sar.processing_artifacts (
    artifact_id         BIGSERIAL PRIMARY KEY,

    job_id              BIGINT REFERENCES sar.processing_job(job_id) ON DELETE CASCADE,
    task_id             BIGINT REFERENCES sar.job_tasks(task_id) ON DELETE CASCADE,
    scene_id            BIGINT REFERENCES sar.sar_scene_master(scene_id) ON DELETE SET NULL,

    artifact_type       VARCHAR(50) NOT NULL,
    file_format         VARCHAR(20),

    hdfs_path           TEXT NOT NULL,
    local_path          TEXT,
    file_size_bytes     BIGINT,

    checksum_sha256     TEXT,
    created_time        TIMESTAMP DEFAULT NOW(),

    min_value           DOUBLE PRECISION,
    max_value           DOUBLE PRECISION,
    mean_value          DOUBLE PRECISION,

    metadata_json       JSONB
);

CREATE INDEX idx_artifact_job ON sar.processing_artifacts(job_id);
CREATE INDEX idx_artifact_task ON sar.processing_artifacts(task_id);
CREATE INDEX idx_artifact_scene ON sar.processing_artifacts(scene_id);
CREATE INDEX idx_artifact_type ON sar.processing_artifacts(artifact_type);




CREATE TABLE sar.publication_provenance (
    prov_id             BIGSERIAL PRIMARY KEY,

    job_id              BIGINT REFERENCES sar.processing_job(job_id) ON DELETE CASCADE,

    paper_title         TEXT,
    experiment_name     TEXT,

    institution         TEXT,
    author_names        TEXT,

    dataset_description TEXT,

    preprocessing_steps TEXT,
    algorithm_details   TEXT,

    hardware_details    TEXT,
    cluster_details     TEXT,

    citations           TEXT,

    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_prov_job ON sar.publication_provenance(job_id);




CREATE VIEW sar.v_job_task_outputs AS
SELECT
    j.job_id,
    j.job_name,
    j.engine,
    j.pipeline_type,
    t.task_id,
    t.task_status,
    t.start_time,
    t.end_time,
    a.artifact_type,
    a.hdfs_path
FROM sar.processing_job j
JOIN sar.job_tasks t ON j.job_id = t.job_id
LEFT JOIN sar.processing_artifacts a ON t.task_id = a.task_id;



UPDATE sar.job_tasks
SET task_status = 'RUNNING',
    assigned_worker = 'worker_01',
    assigned_time = NOW(),
    start_time = NOW(),
    attempt_no = attempt_no + 1
WHERE task_id = (
    SELECT task_id
    FROM sar.job_tasks
    WHERE task_status IN ('QUEUED', 'RETRYING')
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING task_id;
