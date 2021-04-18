"""
Created on Wed Jun 10 16:25:01 2020
@author: lennart

Dimension table sm_seasons
"""


import requests
import json
import pandas as pd 
#import datetime from datetime
import time
from time import gmtime, strftime
from sqlalchemy import create_engine

# import token to acess sportmonks API
from meta_sm_connection import sportmonks_token
my_token = sportmonks_token
print(my_token)

##############
#   SEASON   #
##############

response = requests.get("https://soccer.sportmonks.com/api/v2.0/leagues/82?api_token="+my_token+"&include=season")

print("API CALL Season: " + str(response.status_code))

data = response.json()
data = data['data']

league_data = []
league_data.append(data['id'])
league_data.append(data['name'])
league_data.append(data['logo_path'])
league_data.append(data['current_season_id'])
league_data.append(data['season']['data']['name'])
league_data.append(data['season']['data']['is_current_season'])
league_data.append(data['current_stage_id'])
league_data.append(data['current_round_id'])

response = requests.get("https://soccer.sportmonks.com/api/v2.0/seasons/"+str(data['current_season_id'])+"?api_token="+my_token+"&include=rounds")
print("API CALL Round Name: " + str(response.status_code))

round_data = response.json()
round_data = round_data['data']['rounds']

for i in round_data['data']:
    if (i['id']) == (data['current_round_id']):
        league_data.append(i['name'])
           
league_data.append(strftime("%Y-%m-%d %H:%M:%S", time.localtime()))    
        
        
df_seasons = pd.DataFrame(columns=['league_id','league_name', 'league_logo_path', 'season_id'
                                   , 'season_name', 'is_current_season', 'current_stage_id'
                                   , 'current_round_id', 'current_round_name', 'load_ts'], data=[league_data])

# access mySQL Databse
from meta_db_connection_ftsy import db_user, db_pass, db_port, db_name
engine_input = 'mysql+mysqlconnector://'+db_user+':'+db_pass+'@localhost:'+db_port+'/'+db_name
engine = create_engine(engine_input, echo=False)   

try:
    df_seasons.to_sql(name='sm_seasons', con=engine, index=False, if_exists='fail')
    with engine.connect() as con:
        con.execute('ALTER TABLE `sm_seasons` ADD PRIMARY KEY (`season_id`);')
    message = 'Table sm_seasons created'

except:
    df_seasons.to_sql(name='tmp_sm_seasons', con=engine, index=False, if_exists='replace')
    with engine.connect() as con:
        con.execute('ALTER TABLE `tmp_sm_seasons` ADD PRIMARY KEY (`season_id`);')   
        con.execute('INSERT INTO sm_seasons SELECT * FROM tmp_sm_seasons t2 ON DUPLICATE KEY UPDATE league_logo_path = t2.league_logo_path, is_current_season = t2.is_current_season, current_stage_id = t2.current_stage_id, current_round_id = t2.current_round_id, current_round_name = t2.current_round_name, load_ts = t2.load_ts;')    
        con.execute('DROP TABLE tmp_sm_seasons;')    

    message = "Table sm_seasons updated"

finally:
    con.close()
  
print(message)