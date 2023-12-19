from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first

appName = 'combiningHSLandSOCR'
bucketDataPath = 'gs://ca675-assignment2-bucket'
hslOutputPath = '/output/hslAggregated.csv/part-00000-f809a3c7-33a3-4a83-aec3-8124165219d2-c000.csv'
socrOutputPath = '/output/socrAggregated.csv/part-00000-f337e38a-9085-49aa-9a4b-4e29a3c72572-c000.csv'
outputPath = '/output/combinedHSLandSOCR.csv'

spark = SparkSession.builder.appName(appName).getOrCreate()

# Read the HSL data
hsl_df = spark.read.csv(bucketDataPath + hslOutputPath, header=True, inferSchema=True)

# Pivot the HSL data
pivot_df = hsl_df.groupBy('LOCATION', 'Country').pivot("Indicator").agg(first("Average Value"))

# Read the SOCR data and rename 'Reference area' to 'Country'
socr_df = spark.read.csv(bucketDataPath + socrOutputPath, header=True, inferSchema=True) \
    .withColumnRenamed('Reference area', 'Country')

# Perform the join on the 'Country' column
combined_df = pivot_df.join(socr_df, ["Country"])

# Show the resulting DataFrame
combined_df.show(truncate=False)

combined_df_ordered = combined_df.orderBy("Country")
# Write the output to a CSV file
combined_df_ordered.write.csv(bucketDataPath + outputPath, header=True)

spark.stop()
