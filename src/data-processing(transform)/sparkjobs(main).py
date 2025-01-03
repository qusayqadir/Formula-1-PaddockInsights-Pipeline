# spark jobs ( subscribe to kafka which is the middle to move data between data sources and consumrs) 

# kafka is good for low-latency data pipelines 

# the spark job will be batch ingestion 
# process the data and then store processed data in cloud storage or send it to downstream systems
# like dashboard) 




## use spark to transform and flatten the json into table or sql format so that it can be pushed onto the cloud so that the dashboard can be created

import pandas as pd 
from concurrent.futures import ProcessPoolExecutor

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when, concat, lit, coalesce
from pyspark.sql.types import StructType, StructField, StringType
import os



import os 
import shutil

def rename_output_file(filepath, custome_file_name): 
    for file in os.listdir(filepath):
        if file.startswith("part-") and file.endswith(".csv"): 
            source_file = os.path.join(filepath, file) 
            dest_file = os.path.join(filepath, custome_file_name) 

            shutil.move(source_file, dest_file) #this moves and renames the name 
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
    # header=True; include the column name ]
    rename_output_file(output_path, "yearSummary.csv")


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

                    output_path = f"../../data/transformedData/Results/{year}/{round_folder}/RaceResults"

                    if not os.path.exists(output_path):
                        os.makedirs(output_path, exist_ok=True) 

                    final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
                    rename_output_file(output_path, f"{year}-{round_folder}-RaceResults.csv")
        
    spark.stop()



# round QualiResults
def job3():
    spark = SparkSession.builder\
            .appName("Convert JSON to CSV for QualiResults")\
            .getOrCreate() 
    
    basePath = "../../data/initial-datafetch-api/Results"

    for year in os.listdir(basePath): 
        year_path = os.path.join(basePath, year)
        if not year.isdigit() or int(year) < 2017:
            continue

        if os.path.isdir(year_path): 
            for round_number in os.listdir(year_path): 
                quali_path = os.path.join(year_path, round_number) 
                removeHypen = round_number.replace("-", "")

                qualiResultFile = os.path.join(quali_path, f"{year}-{removeHypen}-QualiResults.json")

                if os.path.isfile(qualiResultFile):
                    # Read the JSON file
                    df = spark.read.option("multiline", "true").json(qualiResultFile)

                    # Flatten the nested JSON structure
                    exploded_df = df.select(
                        col("MRData.RaceTable.season").alias("Season"),
                        col("MRData.RaceTable.round").alias("Round"),
                        col("MRData.RaceTable.Races")
                    ).withColumn("Races", explode(col("Races"))) \
                    .withColumn("QualiResults", explode(col("Races.QualifyingResults")))

                    # Select the final columns with dynamic handling of Q1, Q2, Q3
                    # final_df = exploded_df.select(
                    #     col("Season"),
                    #     col("Round"), 
                    #     col("Races.raceName").alias("RaceName"), 
                    #     col("QualiResults.position").alias("Position"),
                    #     col("QualiResults.Driver.code").alias("DriverID"),
                    #     col("QualiResults.Driver.givenName").alias("FirstName"),
                    #     col("QualiResults.Driver.familyName").alias("FamilyName"), 
                    #     col("QualiResults.Driver.nationality").alias("Nationality"),
                    #     col("QualiResults.Q1").alias("Q1_time") 
                    #     # Dynamically handle Q1, Q2, Q3 with coalesce and concat
                    #     # concat(
                    #     #     coalesce(col("QualiResults.Q1"), lit("")),
                    #     #     lit(",")
                    #     # ).alias("Q1_time"),
                        
                    #     # concat(
                    #     #     coalesce(col("QualiResults.Q2"), lit("")),
                    #     #     lit(",")
                    #     # ).alias("Q2_time"),
                        
                    #     # coalesce(col("QualiResults.Q3"), lit("")).alias("Q3_time")
                    # )

                    final_df = exploded_df.select(
                        col("Season"),
                        col("Round"), 
                        col("Races.raceName").alias("RaceName"), 
                        col("QualiResults.position").alias("Position"),
                        col("QualiResults.Driver.code").alias("DriverID"),
                        col("QualiResults.Driver.givenName").alias("FirstName"),
                        col("QualiResults.Driver.familyName").alias("FamilyName"), 
                        col("QualiResults.Driver.nationality").alias("Nationality"),
                        col("QualiResults.Constructor.name").alias("constructor"),
                        col("QualiResults.Q1").alias("Q1_time"),
                        when((col("Season") >= 2006) & (col("QualiResults.position").cast("int") <= 15),
                            col("QualiResults.Q2")).alias("Q2_time"),
                        when((col("Season") >= 2006) & (col("QualiResults.position").cast("int") <= 10),
                            col("QualiResults.Q3")).alias("Q3_time")
                    )

                    # Write to CSV
                    output_path = f"../../data/transformedData/Results/{year}/{round_number}/QualiResults"
                    if not os.path.exists(output_path):
                        os.makedirs(output_path, exist_ok=True)

                    final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True, nullValue="")

                    # Rename the part file to a readable name
                    rename_output_file(output_path, f"{year}-{round_number}-QualiResults.csv")

    spark.stop()




# round driverChampionshipResults
def job4 (): 
    spark = SparkSession.builder\
            .appName('Convert JSON to CSV for driverStandingsResults')\
            .getOrCreate() 
    
    basePath = "../../data/initial-datafetch-api/Results"

    for year in os.listdir(basePath): 
        year_path = os.path.join(basePath, year)

        if not year.isdigit():
            continue

        if os.path.isdir(year_path): 
            for round_number in os.listdir(year_path): 
                driverStandings_path = os.path.join(year_path, round_number) 
                removeHypen = round_number.replace("-", "")

                driverStandingsResultFile = os.path.join(driverStandings_path, f"{year}-{removeHypen}-Driver-Standings.json")

                if os.path.isfile(driverStandingsResultFile):

                    df = spark.read.option("multiline","true").json(driverStandingsResultFile)

                    exploded_df = df.select(
                                            col("MRData.StandingsTable.season").alias("Season"),
                                            col("MRData.StandingsTable.round").alias("Round"),
                                            explode(col("MRData.StandingsTable.StandingsLists")).alias("StandingsLists")
                                        ).withColumn("DriverStandingResults", explode(col("StandingsLists.DriverStandings")))\
            
                
                final_df = exploded_df.select(
                    col("Season"),
                    col("Round"),
                    col("DriverStandingResults.position").alias("Championship Position"),
                    col("DriverStandingResults.Driver.driverId").alias("DriverID"),
                    col("DriverStandingResults.Driver.givenName").alias("FirstName"),
                    col("DriverStandingResults.Driver.familyName").alias("FamilyName"),
                    col("DriverStandingResults.Driver.nationality").alias("Nationaility"),
                    col("DriverStandingResults.points").alias("Points")
                )

                output_path = f"../../data/transformedData/Results/{year}/{round_number}/DriverStandings"

                if not os.path.exists(output_path):
                    os.makedirs(output_path, exist_ok = True)

                final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True, nullValue="")

                rename_output_file(output_path, f"{year}-{round_number}-DriverStandings.csv")

    spark.stop()




# round constructorsResults
def job5 ():
    spark = SparkSession.builder\
            .appName("Convert JSON to CSV for ConstructorResults")\
            .getOrCreate() 
    
    basePath = "../../data/initial-datafetch-api/Results" 

    for year in os.listdir(basePath):
        year_path = os.path.join(basePath, year) 

        if not year.isdigit():
            continue 

        if os.path.isdir(year_path): 
            for round_number in os.listdir(year_path):
                constructorResultPath = os.path.join(year_path, round_number)
                removeHypen = round_number.replace("-","")

                constructorResultFile = os.path.join(constructorResultPath, f"{year}-{removeHypen}-Constructors-Standings.json")

                if os.path.isfile(constructorResultFile):
                    df = spark.read.option("multiline","true").json(constructorResultFile)


                    exploded_df = df.select(
                        col("MRData.StandingsTable.season").alias("Season"),
                        col("MRData.StandingsTable.round").alias("Round"),
                        explode(col("MRData.StandingsTable.StandingsLists")).alias("StandingsLists")

                    ).withColumn("ConstructorStandings", explode(col("StandingsLists.ConstructorStandings")))


                final_df = exploded_df.select(
                    col("Season"),
                    col("Round"),
                    col("ConstructorStandings.position").alias("Constructor-Position"),
                    col("ConstructorStandings.Constructor.name").alias("Constructor"),
                    col("ConstructorStandings.Constructor.nationality").alias("Nation"),
                    col("ConstructorStandings.points").alias("Points"),
                    col("ConstructorStandings.wins").alias("Wins") 
                )

                output_path = f"../../data/transformedData/Results/{year}/{round_number}/ConstructorStandings"

                if not os.path.exists(output_path):
                    os.makedirs(output_path, exist_ok=True)

                final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True, nullValue = "")

                rename_output_file(output_path, f"{year}-{round_number}-ConstructorStandings.csv") 

    spark.stop() 

if __name__ == "__main__": 

    print("Starting Job 1 : ")
    job1()
    print("Finsied Job 1 ")

    print("Starting Job 2: ")
    job2() 
    print("Finished Job 2")
    
    print("Starting Job 3") 
    job3()
    print("Finished Job 3")

    print("Starting Job 4")
    job4()
    print("Finished Job 4 ")

    print("Starting Job 5")
    job5()
    print("Finished Job 5")

    print("Finished all Spark Jobs")
