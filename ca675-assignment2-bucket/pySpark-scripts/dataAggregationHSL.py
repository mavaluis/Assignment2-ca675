from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, rank, desc
from pyspark.sql.window import Window

# name of the spark app
appName = 'dataAggregationHSL'

# path to the bucket in gcp
bucketDataPath = 'gs://ca675-assignment2-bucket'

# path to the hsl file in bucket
hslCSVPath = '/data/HSL_13122023020055501.csv'

# output path
outputPath = '/output/hslAggregated.csv'

# creates the spark session with the appName 
spark = SparkSession.builder.appName(appName).getOrCreate()

# data file that's gonna contain the information from the hsl file
df = spark.read.csv(bucketDataPath + hslCSVPath, header=True, inferSchema=True)

# filter to get only years recorded more than 2014, and categories only interested in
df_post_filter = df.filter(
    (col("TIME14") >= 2015) & 
    (col("Indicator").isin([
        "Homicide", "Life expectancy at birth", 
        "Household income", "Deaths from suicide, alcohol, drugs", 
        "Household wealth", "Financial insecurity", "Social support", "Housing affordability", "Employment rate"])
    )
)

# decrease the columns to those that we only wanna see, also gotta remove the 
group_by_columns = [
    "LOCATION", "Country", "TYPE_VAR", "Type of indicator", 
    "VARIABLE", "Indicator", "WB", "Current/Future Well-being", 
    "SEX8", "Sex9", "AGE10", "Age11", "EDUCATION12", "Education13"
]

# aggregates data by the average values
df_post_aggregation = df_post_filter.groupBy(group_by_columns) \
                                    .agg(avg("Value").alias("Average Value"))

# gets the desired columns 
desired_columns = ["LOCATION", "Country", "Type of indicator", "Indicator", "Average Value"]

# get the final date file post aggregation
df_final = df_post_aggregation.select(*desired_columns)

# rank the countries by their position in a category with relation to the other countries
windowSpec = Window.partitionBy("Indicator").orderBy(desc("Average Value"))

df_final_with_rank = df_final.withColumn("Rank", rank().over(windowSpec))

df_final_ordered = df_final_with_rank.orderBy("LOCATION", "Indicator", "Rank")

df_final_ordered.write.csv(bucketDataPath + outputPath, header=True)

spark.stop()