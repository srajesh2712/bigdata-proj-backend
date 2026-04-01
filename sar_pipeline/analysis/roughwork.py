from joblib import load
model_path = '../assets/rf_checkpoint_1.joblib'
clf = load(model_path)

print(f"I have {len(clf.estimators_)} trees in my file.")
print(f"The most important feature is index: {clf.feature_importances_.argmax()}")
print(f"Total number of input features: {clf.n_features_in_}")

if hasattr(clf, "feature_names_in_"):
    print("Feature names found in model:")
    print(clf.feature_names_in_)
else:
    print("No names found. You likely used a NumPy array or a list.")
print(f"The classes the model knows: {clf.classes_}")

# Get the depth of the very first tree
first_tree_depth = clf.estimators_[0].get_depth()
print(f"Typical tree depth: {first_tree_depth}")

# Get the average depth of all 300 trees
avg_depth = sum(tree.get_depth() for tree in clf.estimators_) / len(clf.estimators_)
print(f"Average forest depth: {avg_depth:.2f}")

import pprint
pprint.pprint(clf.get_params())