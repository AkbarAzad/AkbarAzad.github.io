# Collect Singapore property agents' transactions data from Housing & Development Board (HDB)
# Data source: data.gov.sg 
# Import packages
# Installing module in VS Code:
# C:\Users\akbarazad\AppData\Local\Programs\Python\Python310\python.exe -m pip <command>
# To run python script in VS Code terminal:
# python agents_sales.py
import requests # For querying APIs
import pandas as pd
from datetime import datetime

# Define endpoint
URL = 'https://data.gov.sg/api/action/datastore_search?resource_id=8a087b7c-a11b-4da8-bbb6-ac933f465acd'

# Define limit
LIMIT = 1000000 # As of 06 July 2022, we have 67K records

# Collect data
def collectData(limit = LIMIT):
    response = requests.get(URL, params = {'limit': LIMIT})
    # Store data in JSON format
    dataJSON = response.json()
    # Output
    return dataJSON

def extractData(dataJSON):
    print(f"Keys in dataJSON are: {dataJSON.keys()}")
    # Get agent records
    records = dataJSON['result']['records']
    # Output
    return records 

def processData(records):
    df = pd.DataFrame(records)
    df["date"] = df["transaction_date"].apply(lambda x: str(datetime.strptime(x, '%b-%Y').date()))
    df["insert_date"] = datetime.now().strftime("%Y%m%d%H%M%S")
    return df

def exportData(df):
    fileName = "agent_sales_records_"
    fileDate = datetime.now().strftime("%Y%m%d%H%M%S")
    df.to_csv(f"C:\\Users\\akbarazad\\OneDrive - PropertyGuru Pte Ltd\\Desktop\\Work\\{fileName}{fileDate}.csv")
    return fileName, fileDate

if __name__ == '__main__':
    dataJSON = collectData(limit = LIMIT)
    records = extractData(dataJSON)
    df = processData(records)
    fileName, fileDate = exportData(df)
    print(f"Number of agent records: {len(records)}")
    print(f"First 5 agent records:\n{records[:5]}")
    print(f"Dataframe of records has {df.shape[0]} rows and {df.shape[1]} columns")
    print(f"First 5 rows of dataframe of records:\n{df.head()}")
    print(f"Records successfully exported as {fileName}{fileDate}.csv")
    