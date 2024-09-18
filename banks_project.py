# Code for ETL operations on Bank data
# Importing the required libraries
import requests                     #La bibliothèque utilisée pour accéder aux informations à partir de l’URL.
from bs4 import BeautifulSoup       # La bibliothèque contenant la fonction utilisée pour le webscraping.BeautifulSoup
import pandas as pd                 # La bibliothèque utilisée pour traiter les données extraites, les stocker dans les formats requis et communiquer avec les bases de données.
import sqlite3                      # Bibliothèque requise pour créer une connexion au serveur de base de données.
import numpy as np                  # La bibliothèque nécessaire à l’opération d’arrondi mathématique telle qu’elle est requise dans les objectifs.
from datetime import datetime       # La bibliothèque contenant la fonction utilisée pour extraire l’horodatage à des fins de journalisation.datetime

# Preliminaries: Installing libraries and downloading data
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = 'Largest_banks_data.csv'

# Task 1: Logging function
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Task 2 : Extraction of data
def extract(url, table_attribs):
    page = requests.get(url).text  #Downloads the content of the webpage from the provided URL
    data = BeautifulSoup(page, 'html.parser')  # Uses BeautifulSoup to parse the HTML content of the page
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')            # Searches for all tables in the page (specifically, <tbody> tags)
    rows = tables[0].find_all('tr')            # Finds all rows (tr tags) in the first table found

    for row in rows:
        col = row.find_all('td')               # Retrieves all columns (td tags) in the row
        if len(col) != 0:                      # Checks if the row contains any columns
            if col[1].find('a') is not None:   # Checks if the second column contains an anchor (<a>) tag
                l = len(col[1].find_all('a'))  # Gets the total number of links in the second column
                # Creates a dictionary with:
                # - "Name": the text of the last link found in the second column, stripped of any extra spaces
                # - "MC_USD_Billion": the content of the third column, converted to a float
                data_dict = {
                    "Name": [col[1].find_all('a')[l-1].text.strip()],
                    "MC_USD_Billion": [float(col[2].contents[0].strip())]
                }
            df1 = pd.DataFrame(data_dict)
            df = pd.concat([df, df1], ignore_index=True)
    return df

# Task 3 : Transformation of data
def transform(df):
    df_exchange_rate = pd.read_csv("exchange_rate.csv")                         # Read exchage data
    exchange_rate = df_exchange_rate.set_index('Currency').to_dict()['Rate']    # Convert to dict

    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    fifth_mc_eur = df['MC_EUR_Billion'][4]

    return df, fifth_mc_eur

# Task 4: Loading to CSV
def load_to_csv(df, csv_path):
    ''' saves the final dataframe as a `CSV` file'''
    return df.to_csv(csv_path)

# Task 5: Loading to Database
def load_to_db(df, sql_connection, table_name):
    ''' Saves the final dataframe as a database table'''
    return df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Task 6: Function to Run queries on Database
def run_query(query_statement, sql_connection):
    ''' Runs the stated query on the database table and
    prints the output on the terminal '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

################################################################## ETL RUN ##########################################################

# Task 7: Verify log entries
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)

df, fifth_mc_eur = transform(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"


query_statement_1 = "SELECT * FROM Largest_banks"
query_statement_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_statement_3 = "SELECT Name from Largest_banks LIMIT 5"
#print(run_query(query_statement_1, sql_connection))
#print(run_query(query_statement_2, sql_connection))
#print(run_query(query_statement_3, sql_connection))
log_progress('Process Complete.')
sql_connection.close()

#print("MC data : \n", df,
#      "\n The market capitalization of the 5th largest bank in billion EUR is : ", fifth_mc_eur)