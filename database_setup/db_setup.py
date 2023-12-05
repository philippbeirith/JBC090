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

print('adding rollup tables')

query_table_rollup = '''
select 
'birth_year' as table_name,
count(*) as total_entries,
count(distinct auhtor_ID) as distinct_users
from birth_year
group by 1
union all
select 
'extrovert_introvert' as table_name,
count(*) as total_entries,
count(distinct auhtor_ID) as distinct_users
from extrovert_introvert
group by 1
union all
select 
'feeling_thinking' as table_name,
count(*) as total_entries,
count(distinct auhtor_ID) as distinct_users
from feeling_thinking
group by 1
union all 
select 
'gender' as table_name,
count(*) as total_entries,
count(distinct auhtor_ID) as distinct_users
from gender
group by 1
union all
select 
'judging_perceiving' as table_name,
count(*) as total_entries,
count(distinct auhtor_ID) as distinct_users
from judging_perceiving
group by 1
union all
select 
'nationality' as table_name,
count(*) as total_entries,
count(distinct auhtor_ID) as distinct_users
from nationality
group by 1
union all
select 
'political_leaning' as table_name,
count(*) as total_entries,
count(distinct auhtor_ID) as distinct_users
from political_leaning
group by 1
union all
select 
'sensing_intuitive' as table_name,
count(*) as total_entries,
count(distinct auhtor_ID) as distinct_users
from sensing_intuitive
group by 1
'''

table_rollup = pd.read_sql_query(query_table_rollup, conn)
table_rollup.to_sql('table_rollup', conn, if_exists='replace', index=False)

print('adding user participation matrix')

query_user_participation = '''
with main_pool as (
select distinct auhtor_ID
from birth_year
union all 
select distinct auhtor_ID
from extrovert_introvert
union all 
select distinct auhtor_ID
from feeling_thinking
union all 
select distinct auhtor_ID
from gender
union all 
select distinct auhtor_ID
from judging_perceiving
union all 
select distinct auhtor_ID
from nationality
union all 
select distinct auhtor_ID
from sensing_intuitive
union all 
select distinct auhtor_ID
from political_leaning
), unique_ids as (
select distinct auhtor_ID
from main_pool
)
select distinct auhtor_ID,
case when auhtor_ID in (select distinct auhtor_ID from birth_year) then 1 else 0 end as birth_year,
case when auhtor_ID in (select distinct auhtor_ID from extrovert_introvert) then 1 else 0 end as extrovert_introvert,
case when auhtor_ID in (select distinct auhtor_ID from feeling_thinking) then 1 else 0 end as feeling_thinking,
case when auhtor_ID in (select distinct auhtor_ID from gender) then 1 else 0 end as gender,
case when auhtor_ID in (select distinct auhtor_ID from judging_perceiving) then 1 else 0 end as judging_perceiving,
case when auhtor_ID in (select distinct auhtor_ID from nationality) then 1 else 0 end as nationality,
case when auhtor_ID in (select distinct auhtor_ID from political_leaning) then 1 else 0 end as political_leaning,
case when auhtor_ID in (select distinct auhtor_ID from sensing_intuitive) then 1 else 0 end as sensing_intuitive,
birth_year + extrovert_introvert + feeling_thinking + gender + judging_perceiving + nationality + political_leaning + sensing_intuitive as total_involvement
from unique_ids
'''

user_participation = pd.read_sql_query(query_user_participation, conn)
user_participation.to_sql('user_participation', conn, if_exists='replace', index=False)

#finished
conn.close()

print('done')

