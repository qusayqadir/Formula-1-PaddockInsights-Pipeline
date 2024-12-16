
import pandas as pd 
import sqlite3 

csv_path = "../../data/transformedData/Year-Summary/part-00000-75aeb1a9-0b4e-4f46-b165-57ce36ee860e-c000.csv"
df = pd.read_csv(csv_path)

conn = sqlite3.connect(":memory:")

df.to_sql("F1_year", conn, index=False, if_exists="replace") 

query = """
    SELECT
        * 
    FROM F1_year
    ORDER BY Season ASC, roundNumber ASC
"""

formatted_df = pd.read_sql_query(query, conn) 

output_path = "../../data/transformedData/Year-Summary/reformattedYearSummary.csv" 
formatted_df.to_csv(output_path, index=False)



