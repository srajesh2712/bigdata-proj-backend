-- This code generated from pgadmin export option

-- Table: sar.processing_job

-- DROP TABLE IF EXISTS sar.processing_job;

CREATE TABLE IF NOT EXISTS sar.processing_job
(
    job_id bigint NOT NULL DEFAULT nextval('sar.processing_job_job_id_seq'::regclass),
    job_name text COLLATE pg_catalog."default" NOT NULL,
    engine character varying(20) COLLATE pg_catalog."default" NOT NULL,
    pipeline_type character varying(50) COLLATE pg_catalog."default" NOT NULL,
    job_status character varying(20) COLLATE pg_catalog."default" NOT NULL DEFAULT 'CREATED'::character varying,
    scene_id integer,
    region_wkt character varying COLLATE pg_catalog."default",
    CONSTRAINT processing_job_pkey PRIMARY KEY (job_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS sar.processing_job
    OWNER to rajesh;
-- Index: idx_job_engine

-- DROP INDEX IF EXISTS sar.idx_job_engine;

CREATE INDEX IF NOT EXISTS idx_job_engine
    ON sar.processing_job USING btree
    (engine COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_job_status

-- DROP INDEX IF EXISTS sar.idx_job_status;

CREATE INDEX IF NOT EXISTS idx_job_status
    ON sar.processing_job USING btree
    (job_status COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;