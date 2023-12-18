from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, rank, desc
from pyspark.sql.window import Window

appName = 'dataAggregationSOCR'
bucketDataPath = 'gs://ca675-assignment2-bucket'
socrCSVPath = '/data/socr_perc_gdp.csv'
outputPath = '/output/socrAggregated.csv'

spark = SparkSession.builder.appName(appName).getOrCreate()

df = spark.read.csv(bucketDataPath + socrCSVPath, header=True, inferSchema=True)

group_by_columns = ['Reference area', 'Unit of measure', 'Expenditure source', 'Measure9']

df_aggregated = df.groupBy(group_by_columns) \
                              .agg(avg("OBS_VALUE").alias("Average %"))

windowSpec = Window.partitionBy('Expenditure source').orderBy(desc("Average %"))

df_with_rank = df_aggregated.withColumn("Rank", rank().over(windowSpec))

df_ordered = df_with_rank.orderBy("Reference area", "Expenditure source", "Rank")

df_ordered.write.csv(bucketDataPath + outputPath, header=True)

spark.stop()
