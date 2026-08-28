-- This code generated from pgadmin export option
-- Table: sar.processing_artifacts

-- DROP TABLE IF EXISTS sar.processing_artifacts;

CREATE TABLE IF NOT EXISTS sar.processing_artifacts
(
    artifact_id bigint NOT NULL DEFAULT nextval('sar.processing_artifacts_artifact_id_seq'::regclass),
    job_id bigint,
    task_id bigint,
    scene_id bigint,
    artifact_type character varying(50) COLLATE pg_catalog."default",
    file_format character varying(20) COLLATE pg_catalog."default",
    hdfs_path text COLLATE pg_catalog."default",
    local_path text COLLATE pg_catalog."default",
    file_size_bytes bigint,
    start_time timestamp without time zone,
    stop_time timestamp without time zone,
    created_time timestamp without time zone DEFAULT now(),
    duration_seconds integer,
    region_wkt text COLLATE pg_catalog."default",
    preprocessing_seconds double precision,
    hdfs_upload_seconds double precision,
    zarr_conversion_seconds double precision,
    pipeline_seconds double precision,
    CONSTRAINT processing_artifacts_pkey PRIMARY KEY (artifact_id),
    CONSTRAINT processing_artifacts_job_id_fkey FOREIGN KEY (job_id)
        REFERENCES sar.processing_job (job_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT processing_artifacts_scene_id_fkey FOREIGN KEY (scene_id)
        REFERENCES sar.sar_scene_master (scene_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT processing_artifacts_task_id_fkey FOREIGN KEY (task_id)
        REFERENCES sar.job_tasks (task_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS sar.processing_artifacts
    OWNER to rajesh;
-- Index: idx_artifact_job

-- DROP INDEX IF EXISTS sar.idx_artifact_job;

CREATE INDEX IF NOT EXISTS idx_artifact_job
    ON sar.processing_artifacts USING btree
    (job_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_artifact_scene

-- DROP INDEX IF EXISTS sar.idx_artifact_scene;

CREATE INDEX IF NOT EXISTS idx_artifact_scene
    ON sar.processing_artifacts USING btree
    (scene_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_artifact_task

-- DROP INDEX IF EXISTS sar.idx_artifact_task;

CREATE INDEX IF NOT EXISTS idx_artifact_task
    ON sar.processing_artifacts USING btree
    (task_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_artifact_type

-- DROP INDEX IF EXISTS sar.idx_artifact_type;

CREATE INDEX IF NOT EXISTS idx_artifact_type
    ON sar.processing_artifacts USING btree
    (artifact_type COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;