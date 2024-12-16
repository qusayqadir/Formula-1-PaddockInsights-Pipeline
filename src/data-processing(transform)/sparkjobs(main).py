# spark jobs ( subscribe to kafka which is the middle to move data between data sources and consumrs) 

# kafka is good for low-latency data pipelines 

# the spark job will be batch ingestion 
# process the data and then store processed data in cloud storage or send it to downstream systems
# like dashboard) 




## use spark to transform and flatten the json into table or sql format so that it can be pushed onto the cloud so that the dashboard can be created

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql.functions import explode 
from pyspark.sql.functions import when 

import pandas as pd 
from concurrent.futures import ProcessPoolExecutor


import os 


# Spark Session 

#summarize seasons: rounds
def job1(): 

    spark = SparkSession.builder\
        .appName("Job1-YearSummary")\
        .getOrCreate() 

    input_path = "../../data/initial-datafetch-api/year/*.json"
    df = spark.read.option("multiline","true").json(input_path)


    flattened_df = df.select(
        col("MRData.RaceTable.season").alias("Season"),
        col("MRData.RaceTable.Races").alias("Races")
    )


    exploded_df = flattened_df.withColumn("Races", explode(col("Races")))
    # exploded_df.show(n=20, truncate = True)


    final_df = exploded_df.select(
        col("Season"), 
        col("Races.round").alias("roundNumber"),
        col("Races.raceName").alias("raceName"),
        col("Races.Circuit.circuitName").alias("circuitName"),
        col("Races.Circuit.Location.country").alias("Country"),
        col("Races.Circuit.Location.locality").alias("City"),
        col("Races.date").alias("Date")

    )

    # final_df.show(n=100,truncate = False)

    output_path = "../../data/transformedData/Year-Summary" 
    final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True) 
    #.coalesce() -> how many files will be created spark creates them in multiple paths 
    # mode="overwrite"; if file exsist it will overide
    # header=True; include the column name 


    spark.stop()

# round RaceResults 
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

                removeHypen = round_folder.replace("-","")
                raceResultFile = os.path.join(round_path, f"{year}-{removeHypen}-RaceResults.json")

                if os.path.isfile(raceResultFile): 
                    df = spark.read.option("multiline", "true").json(raceResultFile) 
                    
                    flattened_df = df.select(
                        col("MRData.RaceTable.season").alias("Season"),
                        col("MRData.RaceTable.round").alias("roundNum"),
                        col("MRData.RaceTable.Races").alias("Races")
                    ) 

                    exploded_df = flattened_df.withColumn("Races", explode(col("Races")))
                    exploded_df = exploded_df.withColumn("Results", explode(col("Races.Results")))

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
                        # when(
                        #     col("Results.FastestLap").isNotNull(),
                        #     col("Results.FastestLap.Time.time")
                        # ).otherwise(None).alias("fastestLap"),
                        col("Results.Time.time").alias("gap"), 
                        col("Results.Constructor.name").alias("constructor")
                    )

                    output_path = f"../../data/transformedData/Results/{year}/{round_folder}"

                    if not os.path.exists(output_path):
                        os.makedirs(output_path, exist_ok=True) 

                    final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        
    spark.stop()



# round QualiResults
# def job3(spark): 


# round driverChampionshipResults
# def job4 (spark): 

# round constructorsResults
# def job5 (spark):




if __name__ == "__main__": 
    print("Starting Job 1 : ")
    job1()
    print("Starting Job 2: ")
    job2() 

    print("Finished all jobs")


