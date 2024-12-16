import json 
import requests
import os 


def historical_data_fetch():    
    type_data = ["year","round","results"]
    base_folder_path = "../../data/initial-datafetch-api/year"

    if not os.path.exists(base_folder_path):
        os.makedirs(base_folder_path)

    header = {"Accept" : "application/json"}

    

    for year in range(2000, 2024 + 1 ,1):
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


    total_round_number = int(total_round_number)

    for round_number in range(1,total_round_number + 1): 
        
        round_number = str(round_number)
        
        base_folder_path = f"../../data/initial-datafetch-api/Results/{year}/Round-{round_number}"
       
        if not os.path.exists(base_folder_path):
            os.makedirs(base_folder_path) 


        baseUrl = f"http://ergast.com/api/f1/{year}/{round_number}/" 
        urlRound = baseUrl + f"results.json"
        urlRoundQualifyng = baseUrl + f"qualifying.json"
        urlRoundConstructors = f"{baseUrl}constructorStandings.json"
        urlRoundDriversStandings = f"{baseUrl}driverStandings.json"


        file_nameRound = f"{year}-Round{round_number}.json"
        file_nameQuali = f"{year}-Round{round_number}-QualiResults.json"
        file_nameConstructorStandings = f"{year}-Round{round_number}-Constructors-Standings.json"
        file_nameDriverStandings = f"{year}-Round{round_number}-Driver-Standings.json"

        folder_pathRound = os.path.join(base_folder_path, file_nameRound)
        folder_pathQuali = os.path.join(base_folder_path, file_nameQuali) 
        folder_pathConstructors = os.path.join(base_folder_path, file_nameConstructorStandings)
        folder_pathDriverStandings = os.path.join(base_folder_path, file_nameDriverStandings)

   
                
        get_resultsRound = requests.get(urlRound, headers=header) 
        get_resultsQuali = requests.get(urlRoundQualifyng, headers=header)
        get_resultsConstructors = requests.get(urlRoundConstructors, headers=header)
        get_resultsDrivers = requests.get(urlRoundDriversStandings, headers=header)



        if get_resultsRound.status_code == 200: 
            get_resultsRound_json = get_resultsRound.json()
            get_resultsQuali_json = get_resultsQuali.json()
            get_resultsConstructors_json = get_resultsConstructors.json() 
            get_resultsDirver_json = get_resultsDrivers.json()
       
            with open(folder_pathRound, "w") as file: 
                json.dump(get_resultsRound_json, file, indent=4)
            
            with open(folder_pathQuali, "w") as file: 
                json.dump(get_resultsQuali_json, file, indent=4)
            
            with open(folder_pathConstructors, "w")  as file: 
                json.dump(get_resultsConstructors_json, file, indent=4) 
         
            with open(folder_pathDriverStandings, "w") as file: 
                json.dump(get_resultsDirver_json, file, indent=4) 

        else:
            print(f"get_results{round_number}and{year}is not working")
        


historical_data_fetch()

