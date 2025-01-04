import requests
import json 

url =  "http://ergast.com/api/f1/1950.json"

data = requests.get(url) 
response = data.json()

with open("test1.json", "w") as file: 
    json.dump(response, file)


new_data = json.loads(response)


# print(new_data[''])


