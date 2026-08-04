Demonstration 

# Scenes Taken 
4 - S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE
6 - S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.SAFE
5 - S1A_IW_GRDH_1SDV_20260208T063005_20260208T063030_063124_07EC55_119E.SAFE
7 - S1A_IW_GRDH_1SDV_20260220T063005_20260220T063030_063299_07F2DE_E43B.SAFE

AOI  Sorted by largest to smallest 
-- Inserting records into the processing_job table for the AOIs

POLYGON((
-3.65 56.35,
-3.25 56.35,
-3.25 56.75,
-3.65 56.75,
-3.65 56.35
))
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ('Jan 2026 GRD Preprocess - Scotland Tile 1', 'PYSNAP', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.65 56.35,-3.25 56.35,-3.25 56.75,-3.65 56.75,-3.65 56.35))');
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ('Jan 2026 GRD Preprocess - Scotland Tile 1', 'SPARK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.65 56.35,-3.25 56.35,-3.25 56.75,-3.65 56.75,-3.65 56.35))');
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ('Jan 2026 GRD Preprocess - Scotland Tile 1', 'DASK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.65 56.35,-3.25 56.35,-3.25 56.75,-3.65 56.75,-3.65 56.35))');


POLYGON((
-3.55 56.45,
-3.35 56.45,
-3.35 56.65,
-3.55 56.65,
-3.55 56.45
))

INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 2', 'PYSNAP', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.55 56.45,-3.35 56.45,-3.35 56.65,-3.55 56.65,-3.55 56.45))');
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 2', 'SPARK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.55 56.45,-3.35 56.45,-3.35 56.65,-3.55 56.65,-3.55 56.45))');
INSERT INTO sar.processing_job ( job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 2', 'DASK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.55 56.45,-3.35 56.45,-3.35 56.65,-3.55 56.65,-3.55 56.45))');


POLYGON((
-3.50 56.50,
-3.40 56.50,
-3.40 56.60,
-3.50 56.60,
-3.50 56.50
))
INSERT INTO sar.processing_job (job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 3', 'PYSNAP', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.50 56.50,-3.40 56.50,-3.40 56.60,-3.50 56.60,-3.50 56.50))');
INSERT INTO sar.processing_job (job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 3', 'SPARK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.50 56.50,-3.40 56.50,-3.40 56.60,-3.50 56.60,-3.50 56.50))');
INSERT INTO sar.processing_job (job_name, engine, pipeline_type, job_status, scene_id, region_wkt) VALUES ( 'Jan 2026 GRD Preprocess - Scotland Tile 3', 'DASK', 'PREPROCESS', 'CREATED', 4, 'POLYGON((-3.50 56.50,-3.40 56.50,-3.40 56.60,-3.50 56.60,-3.50 56.50))');





