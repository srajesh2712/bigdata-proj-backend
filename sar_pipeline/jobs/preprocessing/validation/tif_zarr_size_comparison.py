import os
import fsspec
import rasterio
HADOOP_USER = "btcchl0040"
HDFS_NAMENODE = "namenode"
HDFS_PORT = 8020

os.environ["HADOOP_USER_NAME"] = HADOOP_USER


# Input job IDs
JOB_IDS = [30,31,32,33]


HDFS_BASE = "/user/btcchl0040/dask_preprocessed"


def get_hdfs_size(path, fs):
    """
    Calculates size in MB for a file or directory on HDFS
    """

    if fs.isfile(path):
        return fs.size(path) / (1024 * 1024)

    total_size = 0

    for root, dirs, files in fs.walk(path):
        for f in files:
            full_path = f"{root}/{f}"
            total_size += fs.size(full_path)

    return total_size / (1024 * 1024)



def get_job_paths(job_ids):

    """
    Generates TIFF and Zarr HDFS paths from job IDs
    """

    tiff_paths = []
    zarr_paths = []

    for job_id in job_ids:

        tiff_paths.append(
            f"{HDFS_BASE}/{job_id}/{job_id}_tile.tif"
        )

        zarr_paths.append(
            f"{HDFS_BASE}/{job_id}/{job_id}_tile.zarr"
        )

    return tiff_paths, zarr_paths

def print_file_dimensions(tiff_paths, fs):

    print(
        f"\n{'File Name':<25} | {'Dimensions (Width x Height)':<25}"
    )

    print("-" * 55)


    for t_path in tiff_paths:

        with fs.open(t_path, "rb") as f:

            with rasterio.open(f) as src:

                file_name = os.path.basename(t_path)

                print(
                    f"{file_name:<25} | "
                    f"{src.width:<10} x {src.height:<10}"
                )

if __name__ == "__main__":


    fs = fsspec.filesystem(
        "hdfs",
        host=HDFS_NAMENODE,
        port=HDFS_PORT,
        user=HADOOP_USER
    )


    TIFF_PATHS, ZARR_PATHS = get_job_paths(JOB_IDS)


    print("--- HDFS TIFF Vs ZARR STORAGE ANALYSIS ---")
    # table header below
    print(
        f"{'File Name':<25} | "
        f"{'TIFF Size':<12} | "
        f"{'Zarr Size':<12} | "
        f"{'Savings'}"
    )



    total_tif_mb = 0
    total_zarr_mb = 0


    for t_path, z_path in zip(TIFF_PATHS, ZARR_PATHS):


        if not fs.exists(t_path):
            print(f"Missing TIFF: {t_path}")
            continue


        if not fs.exists(z_path):
            print(f"Missing ZARR: {z_path}")
            continue


        t_size = get_hdfs_size(
            t_path,
            fs
        )


        z_size = get_hdfs_size(
            z_path,
            fs
        )


        total_tif_mb += t_size
        total_zarr_mb += z_size


        file_name = os.path.basename(t_path)


        savings = t_size - z_size

        percentage = (
            savings / t_size
        ) * 100


        print(
            f"{file_name:<25} | "
            f"{t_size:>8.2f} MB | "
            f"{z_size:>8.2f} MB | "
            f"{percentage:>5.1f}%"
        )



    print_file_dimensions(
        TIFF_PATHS,
        fs
    )


    print("-" * 70)


    overall_savings = (
        (total_tif_mb - total_zarr_mb)
        / total_tif_mb
    ) * 100


    print(
        f"{'TOTAL':<25} | "
        f"{total_tif_mb:>8.2f} MB | "
        f"{total_zarr_mb:>8.2f} MB | "
        f"{overall_savings:>5.1f}%"
    )


    print(
        f"\nOverall Storage Reduction: "
        f"{total_tif_mb-total_zarr_mb:.2f} MB saved"
    )
