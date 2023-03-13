import json
import requests
import pandas as pd
from sqlalchemy import create_engine

# Database credentials
db_user_name = 'postgres'
db_password = 'Smiley12'
host = 'localhost'
port = 5432
db_name = 'countries_db'

engine = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')

filenames = ['continents', 'countries', 'languages']

all_files = []
def extract_data():
    for filename in filenames:
        if filename == 'continents':
            url = f'https://raw.githubusercontent.com/annexare/Countries/master/data/{filename}.json'
            response = requests.get(url)
            data = response.json()
            filename = pd.DataFrame({'code' : list(data.keys()), 'continents' : list(data.values())})
            filename.to_csv('data/continents.csv', index=False)
        elif filename == 'countries':
            url = f'https://raw.githubusercontent.com/annexare/Countries/master/data/{filename}.json'
            response = requests.get(url)
            data = response.json()
            columns = ['code'] + list(list(data.values())[0].keys()) + ['None']
            records = [[item[0]] + list(item[1].values()) for item in data.items()]
            countries_df = pd.DataFrame(records, columns=columns)
            countries_df = countries_df[columns[:-1]]
            countries_df.to_csv('data/countries.csv', index=False)
        elif filename == 'languages':
            url = f'https://raw.githubusercontent.com/annexare/Countries/master/data/{filename}.json'
            response = requests.get(url)
            data = response.json()
            columns = ['code'] + list(list(data.values())[0].keys()) + ['none']
            records = [[item[0]] + list(item[1].values()) for item in data.items()]
            languages_df = pd.DataFrame(records, columns=columns)
            languages_df.to_csv('data/languages.csv', index=False)

def load_data():
    for filename in filenames:
        data = pd.read_csv(f'data/{filename}.csv')
        data.to_sql(filename, con=engine, if_exists='replace', index=False)
        print('data successfully written to postgres database')
        #print(data)
    #     with open(f'data/{filename}.json', 'w') as f:
    #         json.dump(data, f)
    #     print(f"Successfully written {filename} to file")
    #     all_files.append(f'{filename}.json')
    #     print(f'Successfully appended {filename} to all files')
    # print(all_files)
    # pd.read_json(all_files[0])
        #for filename in filenames:
            

def main():
    extract_data()
    load_data()
main()