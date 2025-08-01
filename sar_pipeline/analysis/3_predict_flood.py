import rasterio
import numpy as np
from rasterio.windows import Window
from joblib import load
import matplotlib.pyplot as plt
from tqdm import tqdm
# Paths
vv_pre_path ='E:\\Big Data\\Summer Project\\Assam-Flood-June2-2025\\20250521\\subset_0_of_S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D_Orb_Cal_Spk_TC.tif'
vv_post_path='E:\\Big Data\\Summer Project\\Assam-Flood-June2-2025\\20250602\\subset_0_of_S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58_Orb_Cal_Spk_TC.tif'
#vv_pre_path = 'E:\\Big Data\\Summer Project\\Assam-Flood-June5-2024\\Preflood-May24-2024\\20240524\\subset_3_of_S1A_IW_GRDH_1SDV_20240524T115717_20240524T115742_054013_069101_5DC9_Orb_Cal_Spk_TC.tif'
#vv_post_path  = 'E:\\Big Data\\Summer Project\\Assam-Flood-June5-2024\\Flood-June5-2024\\20240605\\subset_0_of_S1A_IW_GRDH_1SDV_20240605T115717_20240605T115742_054188_06970B_2DFB_Orb_Cal_Spk_TC.tif'
model_path = 'rf_checkpoint.joblib'
output_path = 'predicted_flood_assam_june_2025.tif'



# Predict full image
def predict_full_image(tile_size=512):
    # Load model
    print("📥 Loading model...")
    clf = load(model_path)
    with rasterio.open(vv_pre_path) as vv_pre_src, \
         rasterio.open(vv_post_path) as vv_post_src:

        width, height = vv_pre_src.width, vv_pre_src.height
        print(width, height)
        meta = vv_pre_src.meta.copy()
        meta.update({
            'count': 1,
            'dtype': 'uint8',
            'compress': 'lzw'
        })
        total_tiles = ((height - 1) // tile_size + 1) * ((width - 1) // tile_size + 1)
        tile_counter = 0
        with rasterio.open(output_path, 'w', **meta) as dst:
            for row in tqdm(range(0, height, tile_size), desc="🔄 Predicting Rows"):
                for col in range(0, width, tile_size):
                    if row >= height or col >= width:
                        continue  # Skip invalid tiles
                    win_width = max(0, min(tile_size, width - col))
                    win_height = max(0, min(tile_size, height - row))

                    if win_width <= 0 or win_height <= 0:
                        continue  # Skip empty or negative windows

                    window = Window(col, row, win_width, win_height)

                    vv_pre = vv_pre_src.read(1, window=window)
                    vv_post = vv_post_src.read(1, window=window)
                    #print("Min:", np.min(vv_pre), "Max:", np.max(vv_pre), "Mean:", np.mean(vv_pre))

                    vv_diff = vv_post - vv_pre
                    global_stats = [
                            np.std(vv_pre), np.var(vv_pre),
                            np.std(vv_post), np.var(vv_post),
                            np.std(vv_diff), np.var(vv_diff)
                            ]


                    features = []
                    labels = []
                    X = []
                    y = []
                    positions = []
                    for i in tqdm(range(vv_pre.shape[0]), desc="  ↳ Pixels", leave=False):
                                for j in range(vv_pre.shape[1]):
                                    pixel_features = [
                                               vv_pre[i, j],
                                               vv_post[i, j],
                                               vv_diff[i, j]
                                           ] + global_stats
                                    X.append(pixel_features)
                                    positions.append((i, j))

                    pred_img = np.zeros(vv_pre.shape, dtype=np.uint8)

                    if X:
                        X = np.array(X)
                        pred = clf.predict(X)
                        for (i, j), val in zip(positions, pred):
                            pred_img[i, j] = val
                    else:
                        pred_img[:, :] = 0

                    dst.write(pred_img, 1, window=window)
                    tile_counter += 1

            print(f"\n✅ Prediction complete. {tile_counter} tiles processed.")
            print(f"🗂️ Output saved to: {output_path}")

    print(f"\n✅ Prediction complete. Output saved to: {output_path}")

def visualize_tiff(filepath):
    # Open the TIFF
    with rasterio.open(filepath) as src:
        flood_array = src.read(1)  # Assuming single band (1 = flood, 0 = non-flood)
        profile = src.profile

    # Optional: make sure no negative/NaN


    # Define a simple color map: 0 = non-flood (light gray), 1 = flood (blue)
    from matplotlib.colors import ListedColormap

    # Convert to uint8 (if not already)
    flood = flood_array.astype(np.uint8)

    # Define 2-class color map (0 = non-flood, 1 = flood)


    # Display
    plt.figure(figsize=(10, 8))
    plt.imshow(flood, cmap="Blues")
    plt.title("Predicted Flood Mask")
    plt.axis('off')
    plt.colorbar(label="Class")
    plt.show()
# Run prediction
predict_full_image()
visualize_tiff(output_path)
