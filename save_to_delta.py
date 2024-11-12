# save_to_delta.py
import os
from datetime import datetime
import polars as pl
from deltalake import DeltaTable

# Function to save data to Delta Lake format
def save_to_delta_table(data: pl.DataFrame, path: str):
    # Ensure the path exists, or create it (you could use pathlib for this)
    os.makedirs(path, exist_ok=True)
    
    # Create a file path within the directory
    file_path = os.path.join(path, "")
    print("Starting to write into Delta Parquet: ")
    print(datetime.now())
    data.write_delta(file_path)
    print("Starting to write into Delta Parquet: ")
    print(datetime.now())
    print(f"Data written to Delta Lake at {path}")
    # indexing or Z ordering
    # delta_table = DeltaTable.forPath(spark, delta_path)
    # delta_table.optimize().executeZOrderBy("column_name")  # replace "column_name" with an actual column
    # perform any partitioning
