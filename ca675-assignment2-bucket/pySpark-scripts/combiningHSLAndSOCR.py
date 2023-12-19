from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first

# name of the spark app
appName = 'combiningHSLandSOCR'

# path to bucket in gcp 
bucketDataPath = 'gs://ca675-assignment2-bucket'

# path to hsl output file, that contains the aggregated HSL data
hslOutputPath = '/output/hslAggregated.csv/part-00000-f809a3c7-33a3-4a83-aec3-8124165219d2-c000.csv'

# path to sorc output file, that contains the aggregated SOCR data
socrOutputPath = '/output/socrAggregated.csv/part-00000-f337e38a-9085-49aa-9a4b-4e29a3c72572-c000.csv'

# path that will output the (HSL||SOCR) data
outputPath = '/output/combinedHSLandSOCR.csv'

# creates the spark session with the appName
spark = SparkSession.builder.appName(appName).getOrCreate()

# read the HSL data
hsl_df = spark.read.csv(bucketDataPath + hslOutputPath, header=True, inferSchema=True)

# pivot the HSL data
pivot_df = hsl_df.groupBy('LOCATION', 'Country').pivot("Indicator").agg(first("Average Value"))

# read the SOCR data and rename 'Reference area' to 'Country'
socr_df = spark.read.csv(bucketDataPath + socrOutputPath, header=True, inferSchema=True) \
    .withColumnRenamed('Reference area', 'Country')

# perform the join on the 'Country' column
combined_df = pivot_df.join(socr_df, ["Country"])

# print the data frame to check everything is good
combined_df.show(truncate=False)

combined_df_ordered = combined_df.orderBy("Country")

# write the ordered output to a CSV file
combined_df_ordered.write.csv(bucketDataPath + outputPath, header=True)

spark.stop()
