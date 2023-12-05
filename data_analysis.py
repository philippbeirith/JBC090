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

p_matrix = '''
select *
from user_participation
'''
m = (pull(p_matrix))
