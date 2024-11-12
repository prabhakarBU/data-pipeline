from deltalake import DeltaTable
import polars as pl

# Function to save data to Delta Lake format
def read_from_delta_table(path: str):
    # Initialize the DeltaTable for an existing table path
    print("going to read from " + path)
    # delta_table = DeltaTable(path)
    retrieved_data = pl.read_delta(path)
    # Example operation using DeltaTable (e.g., reading data)
    # existing_data = delta_table.to_pandas()
    
    # print(retrieved_data)
    return retrieved_data
