INSERT INTO sar.sar_scene_master
(
    satellite,
    product_type,
    mode,
    polarization,
    acquisition_start,
    acquisition_stop,
    scene_name,
    status
)
VALUES
-- 1
(
    'S1C',
    'SLC',
    'IW',
    'VV,VH',
    '2025-08-24 04:12:53',
    '2025-08-24 04:13:21',
    'S1C_IW_SLC__1SDV_20250824T041253_20250824T041321_003809_00799C_256E.SAFE',
    'DOWNLOADED'
),

-- 2
(
    'S1C',
    'GRD',
    'IW',
    'VV,VH',
    '2025-09-05 04:12:54',
    '2025-09-05 04:13:19',
    'S1C_IW_GRDH_1SDV_20250905T041254_20250905T041319_003984_007ED4_10D5.SAFE',
    'DOWNLOADED'
),

-- 3
(
    'S1C',
    'SLC',
    'IW',
    'VV,VH',
    '2025-09-05 04:12:53',
    '2025-09-05 04:13:21',
    'S1C_IW_SLC__1SDV_20250905T041253_20250905T041321_003984_007ED4_7D8C.SAFE',
    'DOWNLOADED'
),

-- 4
(
    'S1A',
    'GRD',
    'IW',
    'VV,VH',
    '2026-01-15 06:30:07',
    '2026-01-15 06:30:32',
    'S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE',
    'DOWNLOADED'
),

-- 5
(
    'S1A',
    'GRD',
    'IW',
    'VV,VH',
    '2026-02-08 06:30:05',
    '2026-02-08 06:30:30',
    'S1A_IW_GRDH_1SDV_20260208T063005_20260208T063030_063124_07EC55_119E.SAFE',
    'DOWNLOADED'
),

-- 6
(
    'S1A',
    'GRD',
    'IW',
    'VV,VH',
    '2026-01-27 06:30:06',
    '2026-01-27 06:30:31',
    'S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.SAFE',
    'DOWNLOADED'
),

-- 7
(
    'S1A',
    'GRD',
    'IW',
    'VV,VH',
    '2026-02-20 06:30:05',
    '2026-02-20 06:30:30',
    'S1A_IW_GRDH_1SDV_20260220T063005_20260220T063030_063299_07F2DE_E43B.SAFE',
    'DOWNLOADED'
);
