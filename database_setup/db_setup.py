#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 11:15:59 2023

@author: philippbeirith
"""

import pandas as pd
import sqlite3 

print('loading csv files')
#load in the csv files
birth_year_df = pd.read_csv('lai-data/birth_year.csv')
extrovert_introvert_df = pd.read_csv('lai-data/extrovert_introvert.csv')
feeling_thinking_df = pd.read_csv('lai-data/feeling_thinking.csv')
gender_df = pd.read_csv('lai-data/gender.csv')
judging_perceiving_df = pd.read_csv('lai-data/judging_perceiving.csv')
nationality_df = pd.read_csv('lai-data/nationality.csv')
political_leaning_df = pd.read_csv('lai-data/political_leaning.csv')
sensing_intuitive_df = pd.read_csv('lai-data/sensing_intuitive.csv')

#set up db
print('creating db')

conn = sqlite3.connect('database_setup/main.db')

print('loading csv files into db')

birth_year_df.to_sql('birth_year', conn, if_exists='replace', index=False)
extrovert_introvert_df.to_sql('extrovert_introvert', conn, if_exists='replace', index=False)
feeling_thinking_df.to_sql('feeling_thinking', conn, if_exists='replace', index=False)
gender_df.to_sql('gender', conn, if_exists='replace', index=False)
judging_perceiving_df.to_sql('judging_perceiving', conn, if_exists='replace', index=False)
nationality_df.to_sql('nationality', conn, if_exists='replace', index=False)
political_leaning_df.to_sql('political_leaning', conn, if_exists='replace', index=False)
sensing_intuitive_df.to_sql('sensing_intuitive', conn, if_exists='replace', index=False)

#finished
conn.close()
