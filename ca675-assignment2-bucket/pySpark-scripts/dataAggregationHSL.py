from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, rank, desc
from pyspark.sql.window import Window

appName = 'dataAggregationHSL'
bucketDataPath = 'gs://ca675-assignment2-bucket'
hslCSVPath = '/data/HSL_13122023020055501.csv'
outputPath = '/output/hslAggregated.csv'

spark = SparkSession.builder.appName(appName).getOrCreate()

df = spark.read.csv(bucketDataPath + hslCSVPath, header=True, inferSchema=True)

df_post_filter = df.filter(
    (col("TIME14") >= 2015) & 
    (col("Indicator").isin([
        "Homicide", "Life expectancy at birth", 
        "Household income", "Deaths from suicide, alcohol, drugs", 
        "Household wealth", "Financial insecurity", "Social support", "Housing affordability", "Employment rate"])
    )
)

group_by_columns = [
    "LOCATION", "Country", "TYPE_VAR", "Type of indicator", 
    "VARIABLE", "Indicator", "WB", "Current/Future Well-being", 
    "SEX8", "Sex9", "AGE10", "Age11", "EDUCATION12", "Education13"
]

df_post_aggregation = df_post_filter.groupBy(group_by_columns) \
                                    .agg(avg("Value").alias("Average Value"))

desired_columns = ["LOCATION", "Country", "Type of indicator", "Indicator", "Average Value"]
df_final = df_post_aggregation.select(*desired_columns)

windowSpec = Window.partitionBy("Indicator").orderBy(desc("Average Value"))

df_final_with_rank = df_final.withColumn("Rank", rank().over(windowSpec))

df_final_ordered = df_final_with_rank.orderBy("LOCATION", "Indicator", "Rank")

df_final_ordered.write.csv(bucketDataPath + outputPath, header=True)

spark.stop()