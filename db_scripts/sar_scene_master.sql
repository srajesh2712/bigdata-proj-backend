-- This code generated from pgadmin export option
-- Table: sar.sar_scene_master

-- DROP TABLE IF EXISTS sar.sar_scene_master;

CREATE TABLE IF NOT EXISTS sar.sar_scene_master
(
    scene_id bigint NOT NULL DEFAULT nextval('sar.sar_scene_master_scene_id_seq'::regclass),
    product_type character varying(50) COLLATE pg_catalog."default" NOT NULL,
    scene_name text COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    CONSTRAINT sar_scene_master_pkey PRIMARY KEY (scene_id),
    CONSTRAINT sar_scene_master_scene_name_key UNIQUE (scene_name)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS sar.sar_scene_master
    OWNER to rajesh;
-- Index: idx_scene_master_product

-- DROP INDEX IF EXISTS sar.idx_scene_master_product;

CREATE INDEX IF NOT EXISTS idx_scene_master_product
    ON sar.sar_scene_master USING btree
    (product_type COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;