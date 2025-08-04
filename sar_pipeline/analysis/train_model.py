import os
import random
from collections import Counter
from sklearn.utils import resample
import numpy as np
import rasterio
from joblib import dump, load
from rasterio.windows import Window
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
import math

vv_pre_path = '/home/btcchl0040/Documents/SAR_Data/20250803_163826/PREPROCESSING/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE/S1A_IW_GRDH_1SDV_20250521T234717_20250521T234742_059299_075C07_507D.SAFE_20250803_163826.tif'
vv_post_path = '/home/btcchl0040/Documents/SAR_Data/20250803_163826/PREPROCESSING/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE/S1A_IW_GRDH_1SDV_20250602T234717_20250602T234742_059474_076219_9E58.SAFE_20250803_163826.tif'

flood_mask_path = '/home/btcchl0040/Documents/SAR_Data/flood_mask.tif'
output_path = 'predicted_flood1.tif'

# Parameters
tile_size = 512
num_samples = 20

with rasterio.open(vv_pre_path) as src_pre:
    vv_pre = src_pre.read(1)
    print("VV Pre shape:", vv_pre.shape)

with rasterio.open(vv_post_path) as src_post:
    vv_post = src_post.read(1)
    print("VV Post shape:", vv_post.shape)

with rasterio.open(flood_mask_path) as src_mask:
    mask = src_mask.read(1)
    print("Flood Mask shape:", mask.shape)

# Step 1: Random sampling of tiles
with rasterio.open(vv_pre_path) as src:
    width, height = src.width, src.height
    profile = src.profile

sample_windows = []
for _ in range(num_samples):
    row = random.randint(0, height - tile_size)
    col = random.randint(0, width - tile_size)
    sample_windows.append(Window(col, row, tile_size, tile_size))


# Step 2: Feature extraction from each sampled window
def extract_features(window):
    print(f'window size {window}')
    with rasterio.open(vv_pre_path) as vv_pre_src, \
            rasterio.open(vv_post_path) as vv_post_src, \
            rasterio.open(flood_mask_path) as mask_src:

        vv_pre = vv_pre_src.read(1, window=window)
        vv_post = vv_post_src.read(1, window=window)
        flood_data = mask_src.read(1, window=window)

        if flood_data.size == 0:
            return None, None

        if vv_pre.shape != flood_data.shape:
            print(f"⚠ Mismatched shape: VV Pre {vv_pre.shape}, Flood Mask {flood_data.shape}")
            return None, None

        # Optional: Skip if any array is too small
        min_shape = min(vv_pre.shape)
        if min_shape < 3:  # too small to pad + iterate safely
            return None, None

        mask = (flood_data != -9999)
        if np.sum(mask) == 0:
            return None, None

        vv_diff = vv_post - vv_pre

        # New features
        global_stats = [
            np.std(vv_pre), np.var(vv_pre),
            np.std(vv_post), np.var(vv_post),
            np.std(vv_diff), np.var(vv_diff)
        ]
        # Prepare empty list to hold feature vectors
        features = []
        labels = []
        X = []
        y = []

        # Iterate through valid pixels only (excluding the 1-pixel pad)
        for i in range(1, vv_pre.shape[0] - 1):
            for j in range(1, vv_pre.shape[1] - 1):
                pixel_features = [
                                     vv_pre[i, j],
                                     vv_post[i, j],
                                     vv_diff[i, j]
                                 ] + global_stats
                if mask[i, j]:
                    X.append(pixel_features)
                    y.append(flood_data[i, j])

    return np.array(X), np.array(y)

def train_model_data(current_estimators,total_trees,X_train,y_train):
    # Step 2: Train in chunks
    while current_estimators < total_trees:
        next_chunk = min(chunk_size, total_trees - current_estimators)
        clf.n_estimators = current_estimators + next_chunk
        print(f"\n🚀 Training trees {current_estimators + 1} to {clf.n_estimators}...")

        clf.fit(X_train, y_train)

        print("💾 Saving checkpoint...")
        dump(clf, model_path)

        current_estimators = clf.n_estimators

    print("\n✅ Training complete!")


X_all, y_all = [], []
for window in sample_windows:
    X, y = extract_features(window)
    print(f"✅ Features extracted from window at ({window.col_off}, {window.row_off})")

    if X is not None:
        X_all.append(X)
        y_all.append(y)

X = np.vstack(X_all)
y = np.hstack(y_all)

flood_idx = np.where(y == 1)[0]
non_flood_idx = np.where(y == 0)[0]


# Balance by under-sampling the majority class
min_class_size = min(len(flood_idx), len(non_flood_idx))
flood_idx_balanced = resample(flood_idx, replace=False, n_samples=min_class_size, random_state=42)
non_flood_idx_balanced = resample(non_flood_idx, replace=False, n_samples=min_class_size, random_state=42)
# Combine balanced indices
balanced_idx = np.concatenate([flood_idx_balanced, non_flood_idx_balanced])
X_balanced = X[balanced_idx]
y_balanced = y[balanced_idx]
print(f"Training samples: {X.shape[0]}")

# Step 3: Train the Random Forest
X_train, X_test, y_train, y_test = train_test_split(X_balanced, y_balanced, test_size=0.2, random_state=42)

# Parameters
chunk_size = 10
total_trees = 300
model_path = '../assets/rf_checkpoint_1.joblib'


# Get total number of CPU cores
total_cores = os.cpu_count()

# Calculate 90% of cores, at least 1
n_jobs = max(1, math.floor(0.9 * total_cores))

print(f'total_cores {total_cores}')
# Step 1: Try to load existing checkpoint
if os.path.exists(model_path):
    print("📂 Loading existing model checkpoint...")
    clf = load(model_path)
    current_estimators = clf.n_estimators
else:
    print("🆕 Starting fresh Random Forest model...")
    clf = RandomForestClassifier(n_estimators=0, warm_start=True, random_state=42, verbose=1,n_jobs=n_jobs)
    current_estimators = 0

train_model_data(current_estimators,total_trees,X_train,y_train)
  
# Step 3: Evaluate
y_pred = clf.predict(X_test)
print(classification_report(y_test, y_pred))

# Optional: save final version as separate file
dump(clf, '../assets/rf_model_final1.joblib')

y_pred = clf.predict(X_test)
print(classification_report(y_test, y_pred))



print(Counter(y))


print(confusion_matrix(y_test, y_pred))

print("Train Accuracy:", clf.score(X_train, y_train))
print("Test Accuracy:", clf.score(X_test, y_test))
