from azure.storage.blob import BlobServiceClient
import os 

def upload_blobStorage(local_directory, container_name, connection_string):
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string) 
    container_client = blob_service_client.get_container_client(container_name)

    try: 
        container_client.create_container()
        print(f"Created new container - {container_name}")
    except Exception as e :
        print(f"{container_name} exsists, proceeding to upload csv files")  

    for curdir, subdir, files in os.walk(local_directory):
        for file in files:
            if file.endswith("csv"): 
                local_file_path = os.path.join(curdir, file) # found the csv path 
                blob_name = os.path.relpath(local_file_path, local_directory).replace("\\","/") #create realtive path for blob  

                try: 
                    with open(local_file_path, "rb") as data: 
                        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
                        print(f"Uploaded - {blob_name}")
                except Exception as e: 
                    print(f"failed to uplaod - {blob_name}")


if __name__ == "__main__": 

    local_directory_results = "../../data/transformedData/Results" 
    local_directory = "../../data/transformedData"
    local_dreictory_yearSummary = "../../data/transformedData/Year-Summary"
    container_name = "transformeddata2025"

    connection_string = "DefaultEndpointsProtocol=https;AccountName=transformeddata2025;AccountKey=bEEdqV/0GR+y4HAJl9cllBkFRppEqGBzYd4MwuiwSVbimGGDl7hvt5OQtBLsVXh6Wmuj7L93pi+U+AStzM6tSw==;EndpointSuffix=core.windows.net"

    # upload_blobStorage(local_directory=local_directory_results, container_name=container_name, connection_string=connection_string)
    upload_blobStorage(local_directory=local_directory, container_name=container_name, connection_string=connection_string)