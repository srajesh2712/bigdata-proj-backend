import rasterio
import numpy as np

snap_file = "/home/btcchl0040/Documents/SAR_Data/validation/Snap_preprocessed.tif"
pysnap_file = "/home/btcchl0040/Documents/SAR_Data/validation/Pysnap_preprocessed.tif"
spark_file = "/home/btcchl0040/Documents/SAR_Data/validation/40_tile_spark_preprocessed.tif"
dask_file = "/home/btcchl0040/Documents/SAR_Data/validation/40_tile_dask_preprocessed.tif"
def read_band(path, band):
    with rasterio.open(path) as src:
        return src.read(band).astype(np.float32)

def metrics(a, b):
    mask = np.isfinite(a) & np.isfinite(b) & (a > 0) & (b > 0)
    a = a[mask]
    b = b[mask]

    a_db = 10 * np.log10(a)
    b_db = 10 * np.log10(b)

    rmse = np.sqrt(np.mean((a_db - b_db) ** 2))
    corr = np.corrcoef(a_db, b_db)[0, 1]

    return rmse, corr


import rasterio

def inspect(path):
    with rasterio.open(path) as src:
        print("\n", path)
        print("Bands:", src.count)
        for i in range(1, src.count+1):
            band = src.read(i)
            print(i, np.nanmin(band), np.nanmax(band))

inspect(snap_file)
inspect(pysnap_file)
inspect(spark_file)
inspect(dask_file)


import rasterio


with rasterio.open(snap_file) as src:
    print("Width:", src.width)
    print("Height:", src.height)
    print("Bands:", src.count)
    print("Driver:", src.driver)
    print("Block size:", src.block_shapes)
    print("Is tiled:", src.is_tiled)


with rasterio.open(pysnap_file) as src:
    print("Width:", src.width)
    print("Height:", src.height)
    print("Bands:", src.count)
    print("Driver:", src.driver)
    print("Block size:", src.block_shapes)
    print("Is tiled:", src.is_tiled)


with rasterio.open(spark_file) as src:
    print("Width:", src.width)
    print("Height:", src.height)
    print("Bands:", src.count)
    print("Driver:", src.driver)
    print("Block size:", src.block_shapes)
    print("Is tiled:", src.is_tiled)

with rasterio.open(dask_file) as src:
    print("Width:", src.width)
    print("Height:", src.height)
    print("Bands:", src.count)
    print("Driver:", src.driver)
    print("Block size:", src.block_shapes)
    print("Is tiled:", src.is_tiled)

import matplotlib.pyplot as plt


def plot_paper_histograms():
    # Load Band 1 for all products
    snap_b1 = read_band(snap_file, 1)
    pysnap_b1 = read_band(pysnap_file, 1)
    spark_b1 = read_band(spark_file, 1)
    dask_b1 = read_band(dask_file, 1)

    # Filter valid positive pixels and convert to dB
    def to_db_valid(arr):
        mask = np.isfinite(arr) & (arr > 0)
        return 10 * np.log10(arr[mask])

    snap_db = to_db_valid(snap_b1)
    pysnap_db = to_db_valid(pysnap_b1)
    spark_db = to_db_valid(spark_b1)
    dask_db = to_db_valid(dask_b1)

    # Plot setup (JSTARS standard: clear fonts, distinct line styles)
    plt.figure(figsize=(10, 5), dpi=300)

    # Common histogram settings
    bins = 100
    hist_kwargs = {'bins': bins, 'density': True, 'histtype': 'step', 'linewidth': 1.5}

    # Plot lines
    plt.hist(snap_db, label='SNAP Desktop', linestyle='-', color='black', **hist_kwargs)
    plt.hist(pysnap_db, label='PySNAP', linestyle='--', color='blue', **hist_kwargs)
    plt.hist(spark_db, label='Spark ', linestyle='-.', color='green', **hist_kwargs)
    plt.hist(dask_db, label='Dask ', linestyle=':', color='red', **hist_kwargs)

    # Formatting for journal publication
    plt.title("Backscattering Coefficient ($\sigma^0$) Distribution Comparison", fontsize=12, fontweight='bold')
    plt.xlabel("Backscattering Intensity [dB]", fontsize=10)
    plt.ylabel("Probability Density", fontsize=10)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend(loc='upper right', fontsize=9)

    plt.tight_layout()

    # Save for LaTeX submission
    plt.savefig("sar_preprocessing_histogram_comparison.png", dpi=300)
    plt.show()


# Run the plotting function
plot_paper_histograms()