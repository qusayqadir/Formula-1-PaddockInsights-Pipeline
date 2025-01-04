# create 4 massive csv files to be used for efficient powerBI analytics, it contains all the 
# information for the years for every round so that it can be quered more efficiently 

import os 
import pandas as pd 
import csv 

def consalidateCSV(): 

    headers = { "ConstructorStandings" : ["Season","Round","Constructor-Position","Constrcutor","Nation","Points","Wins"], 
                "DriverStandings" :  ["Season","Round","Championship Position","DriverID","FirstName", "FamilyName","Nationality","Points"],
                "QualiResults" : ["Season","Round","RaceName","Position","DriverID","FirstName","FamilyName","Nationality","Constructor","Q1_time","Q2_time","Q3_time"],
                "RaceResults" : ["Season","Round","RaceName","Quali Position","Race Position","DriverID","FirtName","FamilyName","Points","Status","Gap","Constructor"]
            }
   

    base_path = "../../data/transformedData/Results" 

    output_files = { "ConstructorStandings" :  "../../data/transformedData/ConstructorStandings.csv", 
                    "DriverStandings" : "../../data/transformedData/DriverStandings.csv" , 
                    "QualiResults" : "../../data/transformedData/QualiResults.csv", 
                    "RaceResults" :  "../../data/transformedData/RaceResults.csv"

    }
    outfiles = {key: open(path, "w", newline="") for key, path in output_files.items()}
    writers = {key: csv.writer(file) for key, file in outfiles.items()}


    for file_type, writer in writers.items():
        writer.writerow(headers[file_type]) 
    
    
    for year in os.listdir(base_path): 
        year_path = os.path.join(base_path, year)

        if not year.isdigit():
            continue
        
        if os.path.isdir(year_path): 
            for round_number in os.listdir(year_path): 
                round_path = os.path.join(year_path, round_number)

                constructorStandings_path = os.path.join(round_path, "ConstructorStandings")
    
                constructorStandingsFile = os.path.join(constructorStandings_path, f"{year}-{round_number}-ConstructorStandings.csv") #read file
                with open(constructorStandingsFile, "r") as infile: 
                    reader = csv.reader(infile) 
                    next(reader) 
                    for row in reader: 
                        writers["ConstructorStandings"].writerow(row) 

                driver_path = os.path.join(round_path, "DriverStandings") 
                driverStandingsFile = os.path.join(driver_path, f"{year}-{round_number}-DriverStandings.csv") 

                if os.path.isfile(driverStandingsFile):
                    with open(driverStandingsFile, "r") as infile: 
                        reader = csv.reader(infile) 
                        next(reader) 
                        for row in reader: 
                            writers["DriverStandings"].writerow(row) 

                quali_path = os.path.join(round_path, "QualiResults")
                qualiPathFile = os.path.join(quali_path, f"{year}-{round_number}-QualiResults.csv")

                if os.path.isfile(qualiPathFile): 
                    with open(qualiPathFile, "r") as infile: 
                        reader = csv.reader(infile) 
                        next(reader) 

                        for row in reader: 
                            writers["QualiResults"].writerow(row)

                
                raceResults_path = os.path.join(round_path, "RaceResults") 
                raceResultsFile = os.path.join(raceResults_path, f"{year}-{round_number}-RaceResults.csv") 

                if os.path.isfile(raceResultsFile): 
                    with open(raceResultsFile, "r") as infile: 
                        reader = csv.reader(infile)
                        next(reader)

                        for row in reader: 
                            writers["RaceResults"].writerow(row)

    for file in outfiles.values():
        file.close()

if __name__ == "__main__": 
    consalidateCSV() 


