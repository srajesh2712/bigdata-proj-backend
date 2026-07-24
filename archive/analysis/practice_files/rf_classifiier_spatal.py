import rasterio
import numpy as np
import random
from rasterio.windows import Window
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import os
from joblib import dump, load
vv_pre_path = 'E:\\Big Data\\Summer Project\\Assam-Flood-June5-2024\\Preflood-May24-2024\\20240524\\subset_3_of_S1A_IW_GRDH_1SDV_20240524T115717_20240524T115742_054013_069101_5DC9_Orb_Cal_Spk_TC.tif'
vv_post_path  = 'E:\\Big Data\\Summer Project\\Assam-Flood-June5-2024\\Flood-June5-2024\\20240605\\subset_0_of_S1A_IW_GRDH_1SDV_20240605T115717_20240605T115742_054188_06970B_2DFB_Orb_Cal_Spk_TC.tif'

flood_mask_path = '../../assets/flood_mask.tif'
output_path = 'predicted_flood.tif'

# Parameters
tile_size = 512
num_samples = 20


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
    with rasterio.open(vv_pre_path) as vv_pre_src, \
         rasterio.open(vv_post_path) as vv_post_src, \
         rasterio.open(flood_mask_path) as mask_src:

        vv_pre = np.pad(vv_pre_src.read(1, window=window), 1, mode='edge')
        vv_post = np.pad(vv_post_src.read(1, window=window), 1, mode='edge')
        flood_mask = np.pad(mask_src.read(1, window=window), 1, mode='edge')

        mask = (flood_mask != -9999)
        if np.sum(mask) == 0:
            return None, None

        vv_diff = vv_post - vv_pre

        # New features
        vv_pre_std = np.std(vv_pre[mask])
        vv_post_std = np.std(vv_post[mask])
        vv_diff_std = np.std(vv_diff[mask])

        vv_pre_var = np.var(vv_pre[mask])
        vv_post_var = np.var(vv_post[mask])
        vv_diff_var = np.var(vv_diff[mask])
        # Prepare empty list to hold feature vectors
        features = []
        labels = []

        # Iterate through valid pixels only (excluding the 1-pixel pad)
        for i in range(1, vv_pre.shape[0] - 1):
            for j in range(1, vv_pre.shape[1] - 1):
                if flood_mask[i, j] == -9999:
                    continue

                # Extract 3×3 neighborhood for each layer
                patch_pre = vv_pre[i - 1:i + 2, j - 1:j + 2].flatten()
                patch_post = vv_post[i - 1:i + 2, j - 1:j + 2].flatten()
                patch_diff = vv_diff[i - 1:i + 2, j - 1:j + 2].flatten()

                # Concatenate all 3×3 values (27 features)
                patch_features = np.concatenate([patch_pre, patch_post, patch_diff])

                # Add global (tile-wide) statistical features
                patch_features = np.concatenate([
                    patch_features,
                    [vv_pre_std, vv_post_std, vv_diff_std],
                    [vv_pre_var, vv_post_var, vv_diff_var]
                ])

                features.append(patch_features)
                labels.append(flood_mask[i, j])



        return np.array(features), np.array(labels)

X_all, y_all = [], []
for window in sample_windows:
    X, y = extract_features(window)
    if X is not None:
        X_all.append(X)
        y_all.append(y)

X = np.vstack(X_all)
y = np.hstack(y_all)

flood_idx = np.where(y == 1)[0]
non_flood_idx = np.where(y == 0)[0]
from sklearn.utils import resample
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
model_path = '../../assets/rf_checkpoint.joblib'

# Step 1: Try to load existing checkpoint
if os.path.exists(model_path):
    print("📂 Loading existing model checkpoint...")
    clf = load(model_path)
    current_estimators = clf.n_estimators
else:
    print("🆕 Starting fresh Random Forest model...")
    clf = RandomForestClassifier(n_estimators=0, warm_start=True, random_state=42, verbose=1)
    current_estimators = 0

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

# Step 3: Evaluate
y_pred = clf.predict(X_test)
print(classification_report(y_test, y_pred))

# Optional: save final version as separate file
dump(clf, 'rf_model_final.joblib')

y_pred = clf.predict(X_test)
print(classification_report(y_test, y_pred))

from collections import Counter
print(Counter(y))
from sklearn.metrics import confusion_matrix
print(confusion_matrix(y_test, y_pred))

print("Train Accuracy:", clf.score(X_train, y_train))
print("Test Accuracy:", clf.score(X_test, y_test))
