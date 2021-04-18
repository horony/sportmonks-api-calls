#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 11 12:23:12 2020
@author: lennart

Orchestrates single python scripts updating dimension tables

"""

import os

print("\n>> DIMENSION TABLE UPDATER <<<\n")

# SEASONS
print('\n>>> SEASONS-SCRIPT <<<\n')
os.system('python3.6 sm_seasons.py')

# SEASONS
print('\n>>> ROUNDS AND FIXTURES-SCRIPT <<<\n')
os.system('python3.6 sm_fixtures.py')

# VENUES
print('\n>>> VENUE-SCRIPT <<<\n')
os.system('python3.6 sm_venues.py')

# TEAMS
print('\n>>> TEAMS-SCRIPT <<<\n')
os.system('python3.6 sm_teams.py')

# SQUADS bzw PLAYERBASE
print('\n>>> PLAYERBASE-SCRIPT <<<\n')
os.system('python3.6 sm_playerbase.py')