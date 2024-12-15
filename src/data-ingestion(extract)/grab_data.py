import json 
import requests
import os 


def historical_data_fetch():    
    type_data = ["year","round","results"]
    APIdata_folderpath = "../../data/initial-datafetch-api/year"
    if not os.path.exists(APIdata_folderpath):
        os.makedirs(APIdata_folderpath)

    header = {"Accept" : "application/json"}

    

    for year in range(1950, 2024 + 1 ,1): 
        url = f"http://ergast.com/api/f1/{year}.json"
        response = requests.get(url, headers=header) 
        response_data = response.json() 

     
        file_name = f"{year}.json"

        folder_path = os.path.join(APIdata_folderpath, file_name)
         # what it would look like (../../data/initial-datafetch-api/test-json.json)
        if os.path.exists(folder_path):
            continue



        if response.status_code == 200: 

            with open(folder_path, "w") as file: 
                json.dump(response_data, file, indent=4) 
        



historical_data_fetch()

