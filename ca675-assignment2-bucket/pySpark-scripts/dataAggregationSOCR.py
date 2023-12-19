from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# name of the spark app
appName = 'dataAggregationSOCR'

# path to bucket in gcp
bucketDataPath = 'gs://ca675-assignment2-bucket'

# path to socrCSV file in bucket
socrCSVPath = '/data/socr_perc_gdp.csv'

# output path
outputPath = '/output/socrAggregated.csv'

# creates the spark session with the appName
spark = SparkSession.builder.appName(appName).getOrCreate()

# data file that's gonna contain the information from the socrCSV
df = spark.read.csv(bucketDataPath + socrCSVPath, header=True, inferSchema=True)

# group by the reference area
group_by_columns = ['Reference area']

# group by the reference area and the OBS_VALUE only (don't really care about the other columns) also want to rename the OBS_VALUE to what it actually represents, which won't be clear without the columns just removed
df_aggregated = df.groupBy(group_by_columns) \
                              .agg(avg("OBS_VALUE").alias("% of GDP to Social Programs"))

# order it by the country name in ABC order
df_ordered = df_aggregated.orderBy("Reference area")

# write it to the output path found in the bucket
df_ordered.write.csv(bucketDataPath + outputPath, header=True)

spark.stop()
