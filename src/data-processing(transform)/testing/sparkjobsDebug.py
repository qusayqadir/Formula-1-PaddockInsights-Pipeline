from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql.functions import explode 
from pyspark.sql.functions import when 

import pandas as pd 
from concurrent.futures import ProcessPoolExecutor


import os 

def job2(): 

    spark = SparkSession.builder\
        .appName("Job2-RaceResults")\
        .getOrCreate()

    base_path = "../../data/initial-datafetch-api/Results"

    for year in os.listdir(base_path):
        year_path = os.path.join(base_path, year) 
        if os.path.isdir(year_path):
            for round_folder in os.listdir(year_path):
                round_path = os.path.join(year_path, round_folder)

                removeHypen = round_folder.replace("-", "")
                raceResultFile = os.path.join(round_path, f"{year}-{removeHypen}-RaceResults.json")

                if os.path.isfile(raceResultFile): 
                    print(f"Processing file: {raceResultFile}")
                    
                    # Load the JSON file
                    df = spark.read.option("multiline", "true").json(raceResultFile)

                    # Debugging Step 1: Print the schema of the loaded DataFrame
                    print(f"Schema for file {raceResultFile}:")
                    df.printSchema()

                    # Debugging Step 2: Show a sample of the raw JSON data
                    print(f"Sample data for file {raceResultFile}:")
                    df.show(5, truncate=False)

                    # Flatten the data
                    flattened_df = df.select(
                        col("MRData.RaceTable.season").alias("Season"),
                        col("MRData.RaceTable.round").alias("roundNum"),
                        col("MRData.RaceTable.Races").alias("Races")
                    )

                    # Debugging Step 3: Print schema and sample data after flattening
                    print(f"Schema after flattening for file {raceResultFile}:")
                    flattened_df.printSchema()
                    print(f"Sample data after flattening for file {raceResultFile}:")
                    flattened_df.show(5, truncate=False)

                    # Explode the nested Races array
                    exploded_df = flattened_df.withColumn("Races", explode(col("Races")))
                    exploded_df = exploded_df.withColumn("Results", explode(col("Races.Results")))

                    # Debugging Step 4: Show schema and data after explosion
                    print(f"Schema after exploding Races and Results for file {raceResultFile}:")
                    exploded_df.printSchema()
                    print(f"Sample data after exploding Races and Results for file {raceResultFile}:")
                    exploded_df.show(5, truncate=False)

                    # Final selection
                    final_df = exploded_df.select(
                        col("Season"),
                        col("roundNum"),
                        col("Races.raceName").alias("raceNum"), 
                        col("Results.grid").alias("qualiPosition"), 
                        col("Results.position").alias("racePosition"),
                        col("Results.Driver.code").alias("driverID"),
                        col("Results.Driver.givenName").alias("firstName"),
                        col("Results.Driver.familyName").alias("familyName"), 
                        col("Results.Driver.nationality").alias("nationality"),
                        col("Results.points").alias("totalPoints"),
                        col("Results.status").alias("status"),
                        when(
                            col("Results.FastestLap").isNotNull(),
                            col("Results.FastestLap.Time.time")
                        ).alias("fastestLap"),
                        col("Results.Time.time").alias("gap"), 
                        col("Results.Constructor.name").alias("constructor")
                    )

                    # Debugging Step 5: Show schema and data of final DataFrame
                    print(f"Schema of final DataFrame for file {raceResultFile}:")
                    final_df.printSchema()
                    print(f"Sample data of final DataFrame for file {raceResultFile}:")
                    final_df.show(5, truncate=False)

                    # Save the output
                    output_path = f"../../data/transformedData/Results/{year}/{round_folder}"

                    if not os.path.exists(output_path):
                        os.makedirs(output_path, exist_ok=True) 

                    final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        
    spark.stop()


job2()