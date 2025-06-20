import rasterio
import rasterio
import numpy as np
import matplotlib.pyplot as plt
print(rasterio.__version__)


file_1  = 'export1.tif'
file_2 = 'S1A_IW_GRDH_1SDV_20230805T114911_20230805T114936_049740_05FB26_334C_Cal_Spk_TC.tif'
tif_path = f'E:/Big Data/Summer Project/AssamFlood2023/{file_1}'


with rasterio.open(tif_path) as src:
    print("Width:", src.width)
    print("Height:", src.height)
    print("CRS:", src.crs)
    print("Transform:", src.transform)

    # Read a small window from top-left (adjust coordinates if needed)
    window = rasterio.windows.Window(100, 100, 1510, 1512)
    backscatter = src.read(1, window=window)
    if np.any(np.isnan(backscatter)):
        print("Warning: NaN values found in backscatter window!")


    backscatter_db = 10 * np.log10(np.clip(backscatter, 1e-6, None))

    print("Backscatter min/max:", np.min(backscatter), np.max(backscatter))

    # Normalize for better visualization
    plt.imshow(backscatter_db, cmap='Blues_r', vmin=-25, vmax=0)  # Adjust if needed
    plt.colorbar(label='Backscatter (dB)')
    plt.title("VV Backscatter (subset)")
    plt.show()
