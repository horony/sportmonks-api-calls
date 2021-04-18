 #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 23:29:35 2020
@author: lennart

"""


import requests
import pandas as pd 
from datetime import datetime
import pytz
from sqlalchemy import create_engine
import sys

def isNone(s,d):
    if s is None:
        return d
    else:
        return s

# import token to acess sportmonks API
from meta_sm_connection import sportmonks_token
my_token = sportmonks_token

#############
#   META    #
#############

print('\n>>> META <<<\n')

# access mySQL Databse
from meta_db_connection_ftsy import db_user, db_pass, db_port, db_name
engine_input = 'mysql+mysqlconnector://'+db_user+':'+db_pass+'@localhost:'+db_port+'/'+db_name
engine = create_engine(engine_input, echo=False)        

# get values for season and current round
with engine.connect() as con:
    sql_select = con.execute('SELECT spieltag FROM parameter')
    sql_first_row = sql_select.fetchone()
    aktueller_fantasy_spieltag =  sql_first_row['spieltag']
    
    sql_select = con.execute('SELECT season_id FROM sm_seasons WHERE is_current_season = 1')
    sql_first_row = sql_select.fetchone()
    aktuelle_buli_season =  sql_first_row['season_id']
        
print("Aktueller Spieltag Fantasy laut DB: ", aktueller_fantasy_spieltag)
print("Aktueller Season ID laut DB: ", aktuelle_buli_season)
league_id = 82
season_id = aktuelle_buli_season
season_id = int(season_id)
season_id = 17361

# query sportsmonks API
response = requests.get("https://soccer.sportmonks.com/api/v2.0/seasons/"+str(season_id)+"?api_token="+my_token+"&tz=Europe/Amsterdam&include=fixtures.round")
print("API Call League & Season: " + str(response.status_code))
data = response.json()

current_round_id = data['data']['current_round_id']
season_name = data['data']['name']
fixture_extract = data['data']['fixtures']['data']

fixture_ids_all = []

# Start of workflow control. Control which matchdays should be parsed by the script.
print("Script executed interacitvly: " + str(sys.stdin.isatty()))

# if the script is started manually via console...
if sys.stdin.isatty() is True:
    
    # options
    print("Welcher Spieltag soll geupdated werden?\n")
    print("Optionen (Input = Beschreibung):\n")
    print("all = alle Spieltage | all_till_today = alle Spieltage < aktueller Fantasy Spieltag | number e.g. 1 or 29 = exakter Spieltag")
    input_choice = input()
    
    # OPTION 1: Full update of all matchdays
    if input_choice == 'all':
        print("Update alle Spieltage!")
        for i in fixture_extract:
           fixture_ids_all.append(str(i['id']))    
    
    # OPTION 2: Update of all matchdays which are finished     
    elif input_choice == 'all_till_today':
        print("Update alle bislang abgeschlossenen Fantasy-Spieltage!")
        
        with engine.connect() as con:
            sql_select = con.execute('SELECT spieltag FROM parameter')    
            sql_first_row = sql_select.fetchone()
            aktueller_spieltag =  sql_first_row['spieltag']
            print("Alle Spieltag kleiner als ", aktueller_spieltag)

        for i in fixture_extract:
            if i['round']['data']['name'] < aktueller_spieltag:
                fixture_ids_all.append(str(i['id']))        

    # OPTION 3: Update a specific matchday
    elif int(input_choice) in list(range(1, 34)):
        print("Update Spieltag " + str(input_choice) + "!")
        for i in fixture_extract:
            if i['round']['data']['name'] == int(input_choice):
                fixture_ids_all.append(str(i['id']))  
                
    else:
        sys.exit("Keine gültige Eingabe!")
        
    print("Fixtures gefunden: " + str(len(fixture_ids_all)))

# if the script is executed in a Cronjob...
elif sys.stdin.isatty() is False:
    
    print("Python Now(): ", datetime.now(pytz.timezone('Europe/Amsterdam')).date(), " Type: ", type(datetime.now(pytz.timezone('Europe/Amsterdam')).date()))

    with engine.connect() as con:
        sql_select = con.execute('SELECT spieltag FROM parameter')    
        sql_first_row = sql_select.fetchone()
        aktueller_fantasy_spieltag =  sql_first_row['spieltag']
        print("Aktueller Spieltag Fantasy: ", aktueller_fantasy_spieltag)
    
    for i in fixture_extract:
        if i['round']['data']['name'] == aktueller_fantasy_spieltag and datetime.strptime(i['time']['starting_at']['date_time'], '%Y-%m-%d %H:%M:%S') <= datetime.now():#pytz.timezone('Europe/Amsterdam')):
            fixture_ids_all.append(str(i['id']))
    
    print("Fixtures gefunden: " + str(len(fixture_ids_all)))

# cut the fixtures into smaller batches to fit API requirements             
fixture_ids_collection = []
while len(fixture_ids_all) > 0:
    fixture_ids_collection.append(fixture_ids_all[0:9])    
    del fixture_ids_all[0:9]
print("Erstellte Batches: " + str(len(fixture_ids_collection)))

if len(fixture_ids_collection) == 0:
  quit()

###########################
#   PARSE PLAYER STATS    #
###########################
  
print('\n>>> PLAYER STATS <<<\n')

player_list = []
fixture_list = []

for spieltag in fixture_ids_collection:
    
    # Split up fixture batch in order to parse it
    spieltag_number = fixture_ids_collection.index(spieltag)
    fixture_ids_str = ( ','.join(fixture_ids_collection[spieltag_number]))
    
    # if in cronjob query live api
    if sys.stdin.isatty() is False:
        response_player_stats = requests.get("https://soccer.sportmonks.com/api/v2.0/livescores/now?api_token="+my_token+"&include=lineup.player,bench.player,sidelined.player,round,stats")
    # ... else fixtures api
    elif sys.stdin.isatty() is True:
        response_player_stats = requests.get("https://soccer.sportmonks.com/api/v2.0/fixtures/multi/"+fixture_ids_str+"?api_token="+my_token+"&include=lineup.player,bench.player,sidelined.player,round,stats")
    print("API Call Multi Fixtures 2: " + str(response_player_stats.status_code))
    
    data_player_stats = response_player_stats.json()
    player_stats_batch = data_player_stats['data']
    
    # get data from each fixture
    for match in player_stats_batch:
    
        # players can be in the (1) lineup (-> active), (2) benched (-> active) or (3) sidelined
        fixture_lineup = match['lineup']['data']
        fixture_bench = match['bench']['data']    
        fixture_active = fixture_lineup + fixture_bench
        
        # metadata from each fixture
        round_id = match['round']['data']['id']    
        round_name = match['round']['data']['name']    
        stage_id = match['round']['data']['stage_id']
        
        localteam_id = match['localteam_id']
        visitorteam_id = match['visitorteam_id']
        localteam_score = match['scores']['localteam_score']
        visitorteam_score = match['scores']['visitorteam_score']
        
        fixture_status = match['time']['status']
        
        fixture_kickoff_dt = datetime.strptime(match['time']['starting_at']['date'], '%Y-%m-%d').date()
        fixture_kickoff_ts = datetime.strptime(match['time']['starting_at']['date_time'], '%Y-%m-%d %H:%M:%S')
    
        # interate all active players
        for player in fixture_active:
            player_data = []  
            column_names = []
                
            # match data
            player_data.append(season_name)
            column_names.append('season_name')
            player_data.append(round_name)
            column_names.append('round_name')
            player_data.append(fixture_kickoff_ts)
            column_names.append('fixture_kickoff_ts')
            player_data.append(fixture_status)
            column_names.append('fixture_status')
            player_data.append(player['fixture_id'])
            column_names.append('fixture_id')          
            
            # player data
            player_data.append(player['player_id'])
            column_names.append('player_id')
            player_data.append(player['player_name'])
            column_names.append('player_name')
            
            player_data.append(localteam_id)
            player_data.append(visitorteam_id)
            column_names.append('localteam_id')
            column_names.append('visitorteam_id')   
            
            player_data.append(player['team_id'])
            column_names.append('own_team_id')
            if player['team_id'] == visitorteam_id:
                player_data.append(localteam_id)
                player_data.append('away')
            else:
                player_data.append(visitorteam_id)
                player_data.append('home')
            column_names.append('opp_team_id')
            column_names.append('match_type')
            player_data.append(player['number'])
            column_names.append('number')           
            player_data.append(1)
            column_names.append('player_active_flg')
            if  player['stats']['other']['minutes_played'] == 0 or player['stats']['other']['minutes_played'] is None:
                player_data.append(0)
            else:
                player_data.append(1)
            column_names.append('appearance')       
            player_data.append(player['type'])
            column_names.append('player_status')
            player_data.append(player['formation_position'])
            column_names.append('player_formation_position')
            player_data.append(player['position'])
            column_names.append('player_position_en')
            if player['position'] == 'G':
                player_data.append('TW')
                player_data.append('Torwart')
            elif player['position'] == 'D':
                player_data.append('AW')
                player_data.append('Abwehr')
            elif player['position'] == 'M':
                player_data.append('MF')
                player_data.append('Mittelfeld')
            elif player['position'] == 'A':
                player_data.append('ST')
                player_data.append('Sturm')
            else:
                player_data.extend([None] * 2)
            column_names.append('player_position_de_short')
            column_names.append('player_position_de_long')
            player_data.append(player['captain'])
            column_names.append('captain')
            player_data.append(None) #Reason
            column_names.append('sidelined_reason')
            
            # stats
            if player['team_id'] == localteam_id:
                player_data.append(localteam_score)
                team_goals_conceded = visitorteam_score
            else:
                player_data.append(visitorteam_score)
                team_goals_conceded = localteam_score
            column_names.append('team_goals_scored')
            
            if player['team_id'] == localteam_id:
                player_data.append(visitorteam_score)
            else:
                player_data.append(localteam_score)    
            column_names.append('team_goals_conceded')
            
            
            if (player['stats']['other']['minutes_played'] is None):
                player_data.append(None)
            elif (player['stats']['other']['minutes_played'] >= 0):
                if team_goals_conceded == 0:
                    player_data.append(1)
                else:
                    player_data.append(0)
            else:
                player_data.append(None)
            column_names.append('clean_sheet')
    
            player_data.append(player['stats']['goals']['scored'])
            
            player_data.append(isNone(player['stats']['goals']['scored'],0)-isNone(player['stats']['other']['pen_scored'],0))
            player_data.append(player['stats']['goals']['assists'])
            player_data.append(player['stats']['goals']['conceded'])
            player_data.append(player['stats']['goals']['owngoals'])
            column_names += ['goals_total','goals_minus_pen', 'assists', 'goals_conceded', 'owngoals']
                
            player_data.append(player['stats']['cards']['yellowcards'])
            player_data.append(player['stats']['cards']['redcards'])
            player_data.append(player['stats']['cards']['yellowredcards'])
            column_names += ['yellowcards', 'redcards', 'yellowredcards']   
                
            player_data.append(player['stats']['dribbles']['attempts'])
            player_data.append(player['stats']['dribbles']['success'])
            if player['stats']['dribbles']['attempts'] is None or player['stats']['dribbles']['success'] is None:
                player_dribbles_failed = None
            else:
                player_dribbles_failed = player['stats']['dribbles']['attempts']-player['stats']['dribbles']['success']
            player_data.append(player_dribbles_failed ) 
            player_data.append(player['stats']['dribbles']['dribbled_past'])
            column_names += ['dribble_attempts','dribbles_success', 'dribbles_failed', 'dribbled_past'] 
        
            player_data.append(player['stats']['duels']['total'])
            player_data.append(player['stats']['duels']['won'])
            if player['stats']['duels']['total'] is None or player['stats']['duels']['won'] is None:
                player_duels_lost = None
            else:
                player_duels_lost = player['stats']['duels']['total'] - player['stats']['duels']['won']            
            player_data.append(player_duels_lost)
            column_names += ['duels_total', 'duels_won', 'duels_lost']
                
            player_data.append(player['stats']['fouls']['drawn'])
            player_data.append(player['stats']['fouls']['committed'])
            column_names += ['fouls_drawn', 'fouls_committed']  
                
            player_data.append(player['stats']['shots']['shots_total'])
            player_data.append(player['stats']['shots']['shots_on_goal'])
            player_data.append(isNone(player['stats']['shots']['shots_on_goal'],0)-isNone(player['stats']['goals']['scored'],0))
            if player['stats']['shots']['shots_total'] is None or player['stats']['shots']['shots_on_goal'] is None:
                player_shots_missed = None
            else:
                player_shots_missed = player['stats']['shots']['shots_total'] - player['stats']['shots']['shots_on_goal']         
            player_data.append(player_shots_missed)
            column_names += ['shots_total','shots_on_goal','shots_on_goal_saved','shots_missed'] 
                
            player_data.append(player['stats']['passing']['total_crosses'])
            player_data.append(player['stats']['passing']['crosses_accuracy'])
            
            if player['stats']['passing']['total_crosses'] is None or player['stats']['passing']['crosses_accuracy'] is None:
                player_crosses_incomplete = None
                player_data.append(player_crosses_incomplete)
            else:
                player_crosses_incomplete = player['stats']['passing']['total_crosses'] - player['stats']['passing']['crosses_accuracy']
                player_data.append(player_crosses_incomplete)   
                
            column_names += ['crosses_total', 'crosses_complete', 'crosses_incomplete'] 
            
            player_data.append(player['stats']['passing']['accurate_passes']) #complete Passes
            player_data.append(player['stats']['passing']['passes']) #complete Passes
            
            if player['stats']['passing']['accurate_passes'] is None or player['stats']['passing']['passes'] is None:
                player_passes_incomplete = None
                player_data.append(player_passes_incomplete)
            else:
                player_passes_incomplete = player['stats']['passing']['passes']-player['stats']['passing']['accurate_passes']
                player_data.append(player_passes_incomplete)
                
            player_data.append(player['stats']['passing']['passes_accuracy'])
            player_data.append(player['stats']['passing']['key_passes'])
            
            column_names += ['passes_complete', 'passes_total', 'passes_incomplete', 'passes_accuracy', 'key_passes']             
            
            player_data.append(player['stats']['other']['blocks'])
            player_data.append(player['stats']['other']['clearances'])
            player_data.append(player['stats']['other']['dispossesed'])
            player_data.append(player['stats']['other']['hit_woodwork'])
            player_data.append(player['stats']['other']['inside_box_saves'])
            player_data.append(player['stats']['other']['interceptions'])
            player_data.append(player['stats']['other']['minutes_played'])
            player_data.append(player['stats']['other']['offsides'])
            player_data.append(player['stats']['other']['pen_committed'])
            player_data.append(player['stats']['other']['pen_missed'])
            player_data.append(player['stats']['other']['pen_saved'])
            player_data.append(player['stats']['other']['pen_scored'])
            player_data.append(player['stats']['other']['pen_won'])
            player_data.append(player['stats']['other']['saves'])
            player_data.append(player['stats']['other']['tackles'])
            player_data.append(isNone(player['stats']['other']['saves'],0)-isNone(player['stats']['other']['inside_box_saves'],0))

            
            column_names += ['blocks', 'clearances', 'dispossessed', 'hit_woodwork', 'inside_box_saves', 'interceptions',
                                 'minutes_played', 'offsides', 'pen_committed', 'pen_missed', 'pen_saved', 'pen_scored', 
                                 'pen_won', 'saves', 'tackles', 'outside_box_saves']
            
            # Meta            
            player_data.append(int(league_id))
            column_names.append('league_id')            
            player_data.append(season_id)
            column_names.append('season_id')
            player_data.append(int(round_id))
            column_names.append('round_id')  
            player_data.append(fixture_kickoff_dt)
            column_names.append('fixture_kickoff_dt')            
            player_data.append(stage_id)
            column_names.append('stage_id')    
            player_data.append(datetime.now(pytz.timezone('Europe/Amsterdam')))
            column_names.append('load_ts')

            #Append complete dataset
            player_list.append(player_data)
    
    
        #sidelined players    
        fixture_sidelined = match['sidelined']['data']    
        
        for player in fixture_sidelined:
            double_player = 'N'
            player_data = []  

            for sublist in player_list:
                if sublist[5] == player['player_id'] and sublist[4] == player['fixture_id']:
                    double_player = 'Y'

            if double_player == 'N': 
                
                # match             
                player_data.append(season_name)
                player_data.append(round_name)
                player_data.append(fixture_kickoff_ts)
                player_data.append(fixture_status)
                player_data.append(player['fixture_id'])
            
                # player
                player_data.append(player['player_id'])
                player_data.append(player['player_name'])
            
                player_data.append(localteam_id)
                player_data.append(visitorteam_id)

            
                player_data.append(player['team_id'])
                if player['team_id'] == visitorteam_id:
                    player_data.append(localteam_id)
                    player_data.append('away')
                else:
                    player_data.append(visitorteam_id)
                    player_data.append('home')

                player_data.append(None) #number
                player_data.append(0) #active_flg
                player_data.append(0) #appearnce
                player_data.append('sidelined')
                player_data.append(None) #formation_position
                player_data.append(player['position'])
                if player['position'] == 'G':
                    player_data.append('TW')
                    player_data.append('Torwart')
                elif player['position'] == 'D':
                    player_data.append('AW')
                    player_data.append('Abwehr')
                elif player['position'] == 'M':
                    player_data.append('MF')
                    player_data.append('Mittelfeld')
                elif player['position'] == 'A':
                    player_data.append('ST')
                    player_data.append('Sturm')
                else:
                    player_data.extend([None] * 2)
                player_data.append(None) #captain
                player_data.append(player['reason']) #Reason
            
                # stats
                if player['team_id'] == localteam_id:
                    player_data.append(localteam_score)
                    team_goals_conceded = localteam_score
                else:
                    player_data.append(visitorteam_score)
                    team_goals_conceded = visitorteam_score
                
                if player['team_id'] == localteam_id:
                    player_data.append(visitorteam_score)
                else:
                    player_data.append(localteam_score)    
            
                player_data.extend([None] * 46)
            
                # meta            
                player_data.append(int(league_id))
                player_data.append(season_id)
                player_data.append(int(round_id))
                player_data.append(fixture_kickoff_dt)
                player_data.append(stage_id)
                player_data.append(datetime.now(pytz.timezone('Europe/Amsterdam')))
                            
                player_list.append(player_data)
                
print('Player Data List n-1:', len(player_data))
print('Column Names:', len(column_names))

df_player_stats = pd.DataFrame(columns=column_names, data=player_list)
df_player_stats['season_id'] = df_player_stats['season_id'].astype(str).astype(int)

print('Seaon ID: ',season_id,type(season_id))
print(df_player_stats.info(verbose=True))
print(df_player_stats.head(3))

# connect to databse

# if the table is new, create it new
try:
    df_player_stats.to_sql(name='sm_player_stats', con=engine, index=False, if_exists='fail')
    with engine.connect() as con:
        con.execute('ALTER TABLE `sm_player_stats` ADD PRIMARY KEY (`player_id`,`fixture_id`,`round_id`,`league_id`,`season_id`);')
    db_message = 'Table sm_player_stats created'

# ... else update it
except:
    df_player_stats.to_sql(name='tmp_sm_player_stats', con=engine, index=False, if_exists='replace')
    with engine.connect() as con:
        con.execute('ALTER TABLE `tmp_sm_player_stats` ADD PRIMARY KEY (`player_id`,`fixture_id`,`round_id`,`league_id`,`season_id`);')   
        
        con.execute('''
                    INSERT INTO sm_player_stats 
                    SELECT * FROM tmp_sm_player_stats t2 
                        ON DUPLICATE KEY UPDATE 
                            fixture_kickoff_ts = t2.fixture_kickoff_ts
                            , fixture_status=t2.fixture_status
                            , player_name=t2.player_name
                            , localteam_id=t2.localteam_id
                            , visitorteam_id=t2.visitorteam_id
                            , opp_team_id=t2.opp_team_id
                            , match_type=t2.match_type
                            , number=t2.number
                            , player_active_flg=t2.player_active_flg
                            , appearance=t2.appearance
                            , player_status=t2.player_status
                            , player_formation_position=t2.player_formation_position
                            , player_position_en=t2.player_position_en
                            , player_position_de_short=t2.player_position_de_short
                            , player_position_de_long=t2.player_position_de_long
                            , captain=t2.captain
                            , sidelined_reason=t2.sidelined_reason
                            , team_goals_scored=t2.team_goals_scored
                            , team_goals_conceded=t2.team_goals_conceded
                            , clean_sheet=t2.clean_sheet
                            , goals_total=t2.goals_total
                            , goals_minus_pen=t2.goals_minus_pen
                            , assists=t2.assists
                            , goals_conceded=t2.goals_conceded
                            , owngoals=t2.owngoals
                            , yellowcards=t2.yellowcards
                            , redcards=t2.redcards
                            , yellowredcards=t2.yellowredcards
                            , dribble_attempts=t2.dribble_attempts
                            , dribbles_success=t2.dribbles_success
                            , dribbles_failed=t2.dribbles_failed
                            , dribbled_past=t2.dribbled_past
                            , duels_total=t2.duels_total
                            , duels_won=t2.duels_won
                            , duels_lost=t2.duels_lost
                            , fouls_drawn=t2.fouls_drawn
                            , fouls_committed=t2.fouls_committed
                            , shots_total=t2.shots_total
                            , shots_on_goal=t2.shots_on_goal
                            , shots_on_goal_saved = t2.shots_on_goal_saved
                            , shots_missed=t2.shots_missed
                            , crosses_total=t2.crosses_total
                            , crosses_complete=t2.crosses_complete
                            , crosses_incomplete=t2.crosses_incomplete
                            , passes_total=t2.passes_total
                            , passes_accuracy=t2.passes_accuracy
                            , passes_complete=t2.passes_complete
                            , passes_incomplete=t2.passes_incomplete
                            , key_passes=t2.key_passes
                            , blocks=t2.blocks
                            , clearances=t2.clearances
                            , dispossessed=t2.dispossessed
                            , hit_woodwork=t2.hit_woodwork
                            , inside_box_saves=t2.inside_box_saves
                            , outside_box_saves=t2.outside_box_saves
                            , interceptions=t2.interceptions
                            , minutes_played=t2.minutes_played
                            , offsides=t2.offsides
                            , pen_committed=t2.pen_committed
                            , pen_missed=t2.pen_missed
                            , pen_saved=t2.pen_saved
                            , pen_scored=t2.pen_scored
                            , pen_won=t2.pen_won
                            , saves=t2.saves
                            , tackles=t2.tackles
                            , fixture_kickoff_dt=t2.fixture_kickoff_dt
                            , load_ts=t2.load_ts
                    ;''')    
        
        con.execute('DROP TABLE tmp_sm_player_stats;')    

    db_message = "Table sm_player_stats updated"

finally:
    con.close()
  
print(db_message)