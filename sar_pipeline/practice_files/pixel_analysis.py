import rioxarray

# Path to the specific measurement file
file_patharr = ["/home/btcchl0040/Documents/SAR_Data/INPUT/S1A_IW_GRDH_1SDV_20260220T063005_20260220T063030_063299_07F2DE_E43B.SAFE/measurement/s1a-iw-grd-vv-20260220t063005-20260220t063030-063299-07f2de-001.tiff",
"/home/btcchl0040/Documents/SAR_Data/9/PREPROCESSING/Jan2026/S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE/Jan2026/S1A_IW_GRDH_1SDV_20260115T063007_20260115T063032_062774_07DF6F_44A0.SAFE_9.tif"]
      
for file_path in file_patharr:
    # Open the raw TIFF
    raw_data = rioxarray.open_rasterio(file_path)
                                     
    print(f"Shape: {raw_data.shape}")
    print(f"Data Type: {raw_data.dtype}")
    print(f"Max DN Value: {raw_data.max().values}")
    print(f"First 5x5 block of pixels:\n{raw_data.values[0, 0:5, 0:5]}")

    # Look at the center of the image instead of the top-left corner
    cy, cx = raw_data.shape[1] // 2, raw_data.shape[2] // 2
    print(f"Pixels at the center (around {cy}, {cx}):")
    print(raw_data.values[0, cy:cy+5, cx:cx+5])
