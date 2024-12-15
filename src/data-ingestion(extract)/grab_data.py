import json 
import requests
import os 


def historical_data_fetch():    
    type_data = ["year","round","results"]
    base_folder_path = "../../data/initial-datafetch-api/year"

    if not os.path.exists(base_folder_path):
        os.makedirs(base_folder_path)

    header = {"Accept" : "application/json"}

    

    for year in range(1950, 2024 + 1 ,1):
        url = f"http://ergast.com/api/f1/{year}.json"
        response = requests.get(url, headers=header)    
        response_data = response.json() 


        total_round_number= response_data['MRData']['total'] # return string 


        file_name = f"{year}.json"

        folder_path = os.path.join(base_folder_path, file_name)
         # what it would look like (../../data/initial-datafetch-api/test-json.json)
        if os.path.exists(folder_path):
            continue



        if response.status_code == 200: 

            with open(folder_path, "w") as file: 
                json.dump(response_data, file, indent=4) 
                get_year_round_results(year, total_round_number) 


def get_year_round_results(year, total_round_number): 

    header = {"Accept" : "application/json"}

    base_folder_path = f"../../data/initial-datafetch-api/Results/{year}"
    
    if not os.path.exists(base_folder_path):
        os.makedirs(base_folder_path) 

     

    total_round_number = int(total_round_number)

    for round_number in range(1,total_round_number + 1): 
        round_number = str(round_number)
       
        baseUrl = f"http://ergast.com/api/f1/{year}/{round_number}/" 
        urlRound = baseUrl + f"results.json"
        urlRoundQualifyng = baseUrl + f"qualifying.json"

        file_nameRound = f"{year}-Round{round_number}.json"
        file_nameQuali = f"{year}-Round{round_number}-QualiResults.json"
        folder_pathRound = os.path.join(base_folder_path, file_nameRound)
        folder_pathQuali = os.path.join(base_folder_path, file_nameQuali) 


        if os.path.exists(folder_pathRound): 
            continue
        
        get_resultsRound = requests.get(urlRound, headers=header) 
        get_resultsQuali = requests.get(urlRoundQualifyng, headers=header)

        if get_resultsRound.status_code == 200: 
            get_resultsRound_json = get_resultsRound.json()
            get_resultsQuali_json = get_resultsQuali.json()
            with open(folder_pathRound, "w") as file: 
                json.dump(get_resultsRound_json, file, indent=4)
            with open(folder_pathQuali, "w") as file: 
                json.dump(get_resultsQuali_json, file, indent=4)
        else:
            print(f"get_results{round_number}and{year}is not working")



historical_data_fetch()

