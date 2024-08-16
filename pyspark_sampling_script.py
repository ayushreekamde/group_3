from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Dataset Subsampling Script") \
    .getOrCreate()

# Read the Parquet file from S3
df = spark.read.parquet("s3://projectdatafile/datasetpar/obis_20240723.parquet")
df.printSchema()

# Print the total count of records
print(df.count())

# Select the columns based on the specified indices
all_columns = df.columns
column_indices = [8, 262, 25, 33, 42, 47, 52, 57, 144, 136, 161, 165, 3, 2, 10, 11, 173, 140, 139, 142, 148]
column_indices = [i for i in column_indices if i < len(all_columns)]
selected_columns = [all_columns[i] for i in column_indices]
newDf = df.select(*selected_columns)
newDf.printSchema()

# Define the latitude and longitude bounds for the Indian coastline
latitude_min = 6.0
latitude_max = 37.0
longitude_min = 68.0
longitude_max = 97.0

# Apply the filter
filtered_df = newDf.filter(
    (newDf["decimalLatitude"] >= latitude_min) &
    (newDf["decimalLatitude"] <= latitude_max) &
    (newDf["decimalLongitude"] >= longitude_min) &
    (newDf["decimalLongitude"] <= longitude_max)
)

# Save the filtered DataFrame to a new Parquet file
# filtered_df.write.mode("overwrite").parquet("s3://path-to-save-filtered-dataset.parquet")

# Calculate the fraction for sampling
total_count = newDf.count()
sample_fraction = min(1.0, 1000000 / total_count)

# Sample the DataFrame
sampledDf = filtered_df.sample(withReplacement=False, fraction=sample_fraction)
sampledDf = sampledDf.limit(1000000)
sampledDf.printSchema()

# Print the count and number of columns of the sampled DataFrame
print(sampledDf.count())
print(len(sampledDf.columns))

# Show the first 10 rows of the sampled DataFrame
sampledDf.show(10)

# Export the sampled DataFrame as CSV with overwrite mode
# output_path_csv = "s3://dbda-project-group3/data/sampled_obis_csv/"
sampledDf.write.mode("overwrite").option("header", "true").csv(output_path_csv)

# Export the sampled DataFrame as Parquet with overwrite mode
output_path_parquet = "s3://yash1999/sampled-parquet-India/"
sampledDf.write.mode("overwrite").parquet(output_path_parquet)

# Stop the Spark session
spark.stop()
