#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 19:18:56 2020
@author: lennart

dimension table venues
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

#################
#   METAINFOS   #
#################

print('\n>>> META <<<\n')

response = requests.get("https://soccer.sportmonks.com/api/v2.0/leagues?api_token="+my_token)
print("API CALL Meta: " + str(response.status_code))

data = response.json()
league_id = data['data'][0]['id']
print("Die League-ID ist: "+ str(league_id))
season_id = data['data'][0]['current_season_id']
print("Die Season-ID ist: "+ str(season_id))
current_round_id = data['data'][0]['current_round_id']


##############
#   VENUES   #
##############

response = requests.get("https://soccer.sportmonks.com/api/v2.0/venues/season/"+str(season_id)+"?api_token="+my_token)
print("API CALL Venues: " + str(response.status_code))

data = response.json()
data = data['data']

venue_data = []

# parse venues
for venue in data:
    venue_list = []
    venue_list.append(venue['id'])
    venue_list.append(venue['name'])
    venue_list.append(venue['city'])
    venue_list.append(venue['address'])
    venue_list.append(venue['coordinates'])
    venue_list.append(venue['capacity'])
    venue_list.append(venue['surface'])
    venue_list.append(venue['image_path'])
    venue_list.append(strftime("%Y-%m-%d %H:%M:%S", time.localtime()))    
    
    venue_data.append(venue_list)
    
df_venues = pd.DataFrame(columns=['id','name', 'city', 'address', 'coordinates', 'capacity', 'surface', 'image_path', 'load_ts'], data=venue_data)

# access mySQL Databse
from meta_db_connection_ftsy import db_user, db_pass, db_port, db_name
engine_input = 'mysql+mysqlconnector://'+db_user+':'+db_pass+'@localhost:'+db_port+'/'+db_name
engine = create_engine(engine_input, echo=False)  

try:
    df_venues.to_sql(name='sm_venues', con=engine, index=False, if_exists='fail')
    with engine.connect() as con:
        con.execute('ALTER TABLE `sm_venues` ADD PRIMARY KEY (`id`);')
    message = 'Table created'

except:
    df_venues.to_sql(name='tmp_sm_venues', con=engine, index=False, if_exists='replace')
    with engine.connect() as con:
        con.execute('ALTER TABLE `tmp_sm_venues` ADD PRIMARY KEY (`id`);')   
        con.execute('INSERT INTO sm_venues SELECT * FROM tmp_sm_venues t2 ON DUPLICATE KEY UPDATE load_ts = t2.load_ts, name = t2.name, city = t2.city, address = t2.address, coordinates = t2.coordinates, capacity = t2.capacity, surface = t2.surface, image_path = t2.image_path;')    
        con.execute('DROP TABLE tmp_sm_venues;')    

    message = "Table updated"

finally:
    con.close()
  
print(message)