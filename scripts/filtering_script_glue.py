from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, coalesce, rand
from pyspark.sql import functions as F

# Initialize SparkContext and GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# If running as part of an AWS Glue Job, include the Job initialization
# job = Job(glueContext)
# job.init('your_job_name', args)

# Read the Parquet file from S3
df = spark.read.parquet("s3://rawdataobis/obis_20240723.parquet")
df.printSchema()

# Print the total count of records
print(df.count())

# Select the columns based on the specified indices
all_columns = df.columns
column_indices = [8, 262, 25, 33, 42, 47, 52, 57, 144, 136, 161, 165, 3, 2, 10, 11, 173, 140]
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

# Remove columns with all null values
non_null_df = filtered_df.dropna(how='all')


# Replace null values with specific values for specific columns
# Replace null values in 'individualCount' with a random integer between 0 and 39999

# Calculate the average values for the columns
# Calculate the average of the columns
avg_min_depth = non_null_df.agg(F.avg(col("minimumDepthInMeters"))).first()[0]
avg_max_depth = non_null_df.agg(F.avg(col("maximumDepthInMeters"))).first()[0]

# Extract the average values
avg_minimum_depth = average_depths["avg_minimumDepth"]
avg_maximum_depth = average_depths["avg_maximumDepth"]


final_df = non_null_df \
    .withColumn(
        "individualCount",
        coalesce(
            col("individualCount"),
            (rand() * 40000).cast("int")  # Generates a random number between 0 and 39999
        ).cast("int")
    ) \
    .withColumn("minimumDepthInMeters", F.coalesce(col("minimumDepthInMeters"), F.lit(avg_min_depth))) \
    .withColumn("maximumDepthInMeters", F.coalesce(col("maximumDepthInMeters"), F.lit(avg_max_depth))) \
    .withColumn("occurrenceStatus", when(col("occurrenceStatus").isNull(), "absent").otherwise(col("occurrenceStatus"))) \
    .withColumn("lifeStage", when(col("lifeStage").isNull(), "NA").otherwise(col("lifeStage")))

# Define the conservation priority based solely on individualCount thresholds
final_df = final_df.withColumn(
    "conservation_priority",
    when(col("individualCount") < 20.0, "High")
    .when(col("individualCount").between(20.0, 13000.0), "Medium")
    .otherwise("Low")
)

# threshold decided based on 25%ile, 50%ile, 75%ile

# Show the schema to verify the new column
final_df.printSchema()

# Show some example records to verify the conservation_priority classification
final_df.select("scientificName", "individualCount", "occurrenceStatus", "conservation_priority").show(10)

# Save the filtered DataFrame to a new csv file
output_path_csv = "s3://projectdatafile/script/final_data.csv"
final_df.coalesce(1).write.mode("overwrite").csv(output_path_csv, header=True)

# If part of an AWS Glue Job, commit the job
job.commit()

# Stop the Spark session
spark.stop()
