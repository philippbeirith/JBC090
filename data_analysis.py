#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 11:28:30 2023

@author: philippbeirith
"""

import pandas as pd
import sqlite3 as sql3

#function to pull data
def pull(query:str):  
    conn = sql3.connect('database_setup/main.db')    
    result = pd.read_sql_query(query, conn)
    conn.close()
    return(result)

#RESULT: there are no users that appear in all tables
user_spread = '''
select count(distinct auhtor_ID)
from  birth_year
inner join sensing_intuitive
using(auhtor_ID)
inner join sensing_intuitive
using(auhtor_ID)
inner join extrovert_introvert
using(auhtor_ID)
inner join feeling_thinking
using(auhtor_ID)
inner join gender
using(auhtor_ID)
inner join judging_perceiving
using(auhtor_ID)
inner join nationality
using(auhtor_ID)
inner join political_leaning
using(auhtor_ID)
'''

#Looking at the research dataset
rd = '''
select distinct auhtor_ID, 
    count(distinct source) number_of_sources,
    count(*) number_of_entries
from research_dataset
group by 1
order by 2 desc
'''
print(pull(rd))

#Looking at test/edge cases
test = ''' select source, count(*) from research_dataset where auhtor_ID = 't2_tjqum' group by 1'''
print(pull(test))

test_2 = ''' select * from birth_year where  auhtor_ID = 't2_tjqum' '''
print(pull(test_2))

#Pull dataset used for research:
pull_rd = ''' select * from research_dataset'''
print(pull(pull_rd))
dataset = (pull(pull_rd))