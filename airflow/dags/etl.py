import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

def extract():
    url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    r = requests.get(url).text
    soup = BeautifulSoup(r, 'html.parser')
    table = soup.find('table',class_="srn-white-background")
    rows = table.tbody.find_all('tr')

    dataframe = pd.DataFrame(columns=['Country','GDP_USD_billion'])

    for i,row in enumerate(rows):
        if i>=3:
            tds = row.find_all('td')
            country = tds[0].a.text
            gdp = tds[2].text
            dataframe = dataframe._append({'Country':country,'GDP_USD_billion':gdp},ignore_index=True)

    dataframe.to_csv('scraped_data.csv', index=False)

def transform():
    dataframe = pd.read_csv('scraped_data.csv')
    dataframe['GDP_USD_billion'] = dataframe['GDP_USD_billion'].replace('—','0')
    dataframe['GDP_USD_billion'] = dataframe['GDP_USD_billion'].str.replace(',','').astype(int)
    dataframe['GDP_USD_billion'] = round(dataframe.GDP_USD_billion/1000,2)
    dataframe.to_csv('transformed_data.csv', index=False)

def load():
    dataframe = pd.read_csv('transformed_data.csv')

    table_name = 'countries_by_gdp'

    try:
        pg_hook = PostgresHook(postgres_conn_id='etl_world_economies')
        print("Connection successful")
        
        table_query = "create table if not exists "+table_name+"(id serial primary key, Country varchar(50), GDP_USD_billion float);"
        pg_hook.run(table_query)
        
        insert_query = "insert into "+table_name+"(Country,GDP_USD_billion) values (%s,%s);"
        for i, row in dataframe.iterrows():
            pg_hook.run(insert_query,parameters=(row['Country'],row['GDP_USD_billion']))

        
        result = pg_hook.get_records('select * from '+table_name+';')
        print(result)
        print("DB Connection closed...")
    
    except Exception as error:
        print(f"ERROR: {error}")





