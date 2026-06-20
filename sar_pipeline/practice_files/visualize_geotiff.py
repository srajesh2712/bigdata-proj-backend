import rasterio
import numpy as np
import matplotlib.pyplot as plt
tif_path = "/home/btcchl0040/Documents/SAR_Data/1775417427.87809/PREPROCESSING/S1A_IW_GRDH_1SDV_20260127T063006_20260127T063031_062949_07E5BF_7B61.SAFE/tile_Q2_1775417427.87809.tif"

with rasterio.open(tif_path) as src:
    print("Band descriptions:", src.descriptions)
    print("----- BASIC INFO -----")
    print("File:", tif_path)
    print("Driver:", src.driver)
    print("Width x Height:", src.width, "x", src.height)
    print("Bands:", src.count)
    print("Dtype:", src.dtypes)
    print("CRS:", src.crs)
    print("Transform:", src.transform)
    print("Bounds:", src.bounds)
    print("Resolution:", src.res)
    print("Nodata:", src.nodata)
    print("Is tiled:", src.is_tiled)
    print("Block shapes:", src.block_shapes)

    print("\n----- TAGS / METADATA -----")
    print(src.tags())

    # Read first band
    band1 = src.read(1)

    print("\n----- BAND 1 STATS -----")
    print("Min:", np.min(band1))
    print("Max:", np.max(band1))
    print("Mean:", np.mean(band1))
    print("Std:", np.std(band1))
    
    # Read first band
    band2 = src.read(2)

    print("\n----- BAND 2 STATS -----")
    print("Min:", np.min(band2))
    print("Max:", np.max(band2))
    print("Mean:", np.mean(band2))
    print("Std:", np.std(band2))
    
    band1 = band1[band1 > 0]
    band2 = band2[band2 > 0]

    plt.hist(band1, bins=200)
    plt.title("Band 1 histogram")
    plt.show()

    plt.hist(band2, bins=200)
    plt.title("Band 2 histogram")
    plt.show()
    
    b1_db = 10 * np.log10(band1[band1>0])
    b2_db = 10 * np.log10(band2[band2>0])

    plt.hist(b1_db, bins=200)
    plt.title("Band 1 (VH) in dB")
    plt.show()

    plt.hist(b2_db, bins=200)
    plt.title("Band 2 (VV) in dB")
    plt.show()
    

    # Count zeros / nodata
    if src.nodata is not None:
        nodata_count = np.sum(band1 == src.nodata)
        print("Nodata pixels:", nodata_count)

    print("Zero pixels:", np.sum(band1 == 0))
