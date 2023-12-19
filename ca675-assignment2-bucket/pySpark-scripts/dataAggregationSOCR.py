from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

appName = 'dataAggregationSOCR'
bucketDataPath = 'gs://ca675-assignment2-bucket'
socrCSVPath = '/data/socr_perc_gdp.csv'
outputPath = '/output/socrAggregated.csv'

spark = SparkSession.builder.appName(appName).getOrCreate()

df = spark.read.csv(bucketDataPath + socrCSVPath, header=True, inferSchema=True)

group_by_columns = ['Reference area']

df_aggregated = df.groupBy(group_by_columns) \
                              .agg(avg("OBS_VALUE").alias("% of GDP to Social Programs"))

df_ordered = df_aggregated.orderBy("Reference area")

df_ordered.write.csv(bucketDataPath + outputPath, header=True)

spark.stop()
