from pyspark.sql import SparkSession 
from pyspark.sql.functions import col 
from pyspark.sql.functions import explode 
import os 



spark = SparkSession.builder\
    .appName("Test")\
    .getOrCreate() 


base_path = "../../data/initial-datafetch-api/Results"
test_path = os.path.join(base_path, "2024/Round-1/2024-Round1-RaceResults.json")

df = spark.read.option("multiline","true").json(test_path)

flattened_df = df.select(
    col("MRData.RaceTable.season").alias("Season"), 
    col("MRData.RaceTable.Races").alias("Races"),
)

flattened_df.show() 

exploded_df = flattened_df.withColumn("Races", explode(col("Races")))
exploded_df = exploded_df.withColumn("Results", explode(col("Races.Results"))) 

final_df = exploded_df.select(
    col("Season"), 
    col("Results.grid").alias("qualiPosition"),
    col("Results.position").alias("racePosition"), 
    col("Results.Driver.code").alias("driver_ID"),
    col("Results.Driver.givenName").alias("firstName"),
    col("Results.Driver.familyName").alias("familyName"),
    col("Results.Driver.nationality").alias("nationality"),
    col("Results.points").alias("totalPoints"),
    col("Results.FastestLap.Time.time").alias("fastestLap"),
    col("Results.Time.time").alias("gap"),
    col("Results.Constructor.name").alias("constructor")
)

final_df.show(truncate=False)