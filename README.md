<h1 align="center">Formula-1-Paddock-Insights-Pipeline </h1>

This codebase focuses on developing the technical side of a data pipeline using python to retrive data from the F1 Ergast REST API, formating and uploading it to Azure Blob Storage  which enables PowerBI for visualization ( restrictions on school and personal azure account ) 

The focus on this project was more about setting up the technical side of the data pipeline. Used cloud storage for future scalability and durability. 
Apache Spaked for processing and transformating the large complex datasets of nested json files efficiently. 
Apache Airflow used as an orchestration tool to automate the data pipeline ingestion, transformation and reupload to the cloud. 

Next Steps: take in more data from some of the other channels ( i.e live telemtery data, or live driver positions during a race) and display that data on the dashboard as well, I would also refactor the code to remove redundency, improve the latency issue with the API calls using multithreading and caching to avoid redundent requests. 

<h3> Current Note: </h3>
Facing issues with airflow web UI dispalying DAGs following some refactor changes, however, the pipeline is still operating. 
