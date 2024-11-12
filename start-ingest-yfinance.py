import os
import yfinance as yf
import polars as pl
from datetime import datetime
from save_to_delta import save_to_delta_table
from save_to_blaze import save_to_blaze
from read_from_delta import read_from_delta_table

# Define a function to fetch data
def fetch_trade_data(ticker, start_date, end_date):
    stock_data = yf.download(ticker, start=start_date, end=end_date)
    df = pl.DataFrame(stock_data.reset_index())
    return df


# Main function for data ingestion and saving to Delta
def main():
    # Fetch example data for a specific stock
    ticker = "AAPL"
    start_date = "2024-10-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    print("start reading: ")
    print(datetime.now())
    data = fetch_trade_data(ticker, start_date, end_date)
    print(type(data))
    print("End reading >> Start Writing: ")
    print(datetime.now())
    # print(data)
    
    # path = "/home/prab/test-python/trade-data-test.csv"
    path = "trade-data-test.csv"
    # data.write_csv(path, include_header=False, separator=",")
    print("End Writing: ")
    print(datetime.now())
    print("Data fetched and written to CSV successfully.")

    # Call the function from save_to_delta.py to save data
    # delta_table_path = "./delta-lake/trade-data"
    delta_table_path = "delta-lake/yfinance-trade-data"
    # save_to_delta_table(data, delta_table_path)
    print("Data saved to Delta Lake format successfully.")
    
    # save to backblaze
    # save_to_blaze("first-bucket")

    ## read and process from parquet
    delta_file_path = os.path.join(delta_table_path, "")
    retrieved_data = read_from_delta_table(delta_file_path)
    print(retrieved_data)


if __name__ == "__main__":
    main()
