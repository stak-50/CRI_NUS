import math
from scipy.io import loadmat
import pandas as pd
import numpy as np
from datetime import datetime
from src.helper import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# current directory absolute path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# parent directory absolute path
APP_DIR = os.path.dirname(CURRENT_DIR)
print("APP_DIR: ", APP_DIR)


def clean_cell(val):
    if isinstance(val, str):
        stripped = val.strip()
        if stripped.lower() in {"nan", "nat", ""}:
            return None
        return stripped
    elif pd.isnull(val):  # catches np.nan, NaT, etc.
        return None
    return val


def save_data_as_parquet(data_df, file_name, chunk_size_mb=500):
    try:
        """
        Saves a DataFrame as one or more Parquet files, each approximately chunk_size_mb in size.

        :param data_df: pandas DataFrame
        :param file_name: base name for output files (e.g. 'sensor_data')
        :param chunk_size_mb: target size per file in MB
        """
        # Estimate size in memory (in bytes)
        estimated_size_bytes = data_df.memory_usage(deep=True).sum()
        estimated_size_mb = estimated_size_bytes / (1024 * 1024)
        print(f"Estimated File Size: ({estimated_size_mb} MB)")

        if estimated_size_mb <= chunk_size_mb:
            # Save as single Parquet file
            output_file = f"{file_name}.parquet"
            data_df.to_parquet(output_file, index=False, compression="snappy")
            print(
                f"Saved single Parquet file: {output_file} ({estimated_size_mb:.2f} MB)"
            )
        else:
            # Split into chunks
            num_chunks = math.ceil(estimated_size_mb / chunk_size_mb)
            rows_per_chunk = math.ceil(len(data_df) / num_chunks)

            for i in range(num_chunks):
                start_idx = i * rows_per_chunk
                end_idx = min((i + 1) * rows_per_chunk, len(data_df))
                chunk_df = data_df.iloc[start_idx:end_idx]

                output_file = f"{file_name}_part_{i+1}.parquet"
                chunk_df.to_parquet(output_file, index=False, compression="snappy")
                print(
                    f"Saved chunk {i+1}/{num_chunks}: {output_file} ({len(chunk_df)} rows)"
                )
        return True
    except Exception as e:
        print(f"Error saving data as parquet: {e}")
        return False


def load_mat_as_dataframe_old(
    mat_file_path: str,
    variable_name: str = None,
) -> pd.DataFrame:
    """
    Loads a MATLAB .mat file (v7.0 or older) using scipy.io.loadmat and returns a pandas DataFrame.

    Args:
        mat_file_path (str): Path to the .mat file.
        variable_name (str, optional): Name of the variable to extract. If None, will extract the first suitable matrix.

    Returns:
        pd.DataFrame: Extracted data.
    """
    # print(f"load_mat_as_dataframe_old: mat_file_path: {mat_file_path}")
    # print(f"load_mat_as_dataframe_old: variable_name: {variable_name}")
    data = loadmat(mat_file_path)
    data = {k: v for k, v in data.items() if not k.startswith("__")}
    # print(f"load_mat_as_dataframe_old: data.keys: {data.keys()}")
    if variable_name:
        if variable_name not in data:
            raise ValueError(f"Variable '{variable_name}' not found in .mat file.")
        matrix = data[variable_name]
        # print(f"load_mat_as_dataframe_old: matrix.shape: {matrix.shape}")
        # print(f"load_mat_as_dataframe_old: matrix: {matrix[:min(10, matrix.shape[0])]}")
    else:
        # Pick the first variable that looks like a 2D matrix
        for key, val in data.items():
            if isinstance(val, np.ndarray) and val.ndim <= 2:
                matrix = val
                break
        else:
            raise ValueError("No matrix-like variable found in .mat file.")

    res_df = pd.DataFrame(matrix)
    return res_df
