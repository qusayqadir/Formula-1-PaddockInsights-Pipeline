'''
schedule and mointor workflow (pipelines)
this will automate tasks: 
    1. ingesting data,
    2. triggering spark jobs
    3. update dashbaords

create a DAG (directed acyclic graph): 
1. read data from kafka
2. tigger spark jobs
3. store processed data in cloud storage 
'''