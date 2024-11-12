import polars as pl
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from langchain.prompts import PromptTemplate

# from langchain.llms import OpenAI
# from langchain_openai import OpenAI
from langchain_openai import ChatOpenAI  # Use ChatOpenAI for chat models
# from langchain.chains import LLMChain

# Initialize Spark with Delta support
builder = (
    SparkSession.builder.appName("DeltaLakeLangChain")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 1. Load Data from Delta Lake
# Define the path to your Delta Lake table
delta_table_path = "delta-lake/yfinance-trade-data"

# Use Polars to read from Delta Lake
# Note: This assumes you've saved the table in Delta format
df = pl.read_delta(delta_table_path)

# Inspect the data
print(df.head())

# 2. Set Up LangChain with OpenAI LLM
# Initialize the OpenAI LLM with your API key
llm = ChatOpenAI(
    model="gpt-4-turbo", api_key=""
)

# Create a basic prompt template
template = """
You are analyzing data from a Delta Lake database.
Here is the data snapshot:

{data_snapshot}

Based on this data, generate a brief summary and identify any notable trends.
"""

prompt = PromptTemplate(input_variables=["data_snapshot"], template=template)

# Initialize an LLM Chain with the prompt
# llm_chain = LLMChain(llm=llm, prompt=prompt)

# Generate a response from the LLM based on the data
# response = llm_chain.run(data_snapshot=data_snapshot)
pipeline = prompt | llm  # This creates a `RunnableSequence`

# 3. Process Data and Generate Responses
# Convert a small subset of the DataFrame to a string to send to the LLM
data_snapshot = df.head(5).to_pandas().to_string()  # Take a small sample for testing
input_data = {"data_snapshot": data_snapshot}

# Run the pipeline with the data snapshot
result = pipeline.invoke(input_data)

# Output the response
print("LLM Analysis:")
print(result)
