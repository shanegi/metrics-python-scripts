#!/usr/bin/env python
# coding: utf-8

# In[11]:


#Step1: Importing the required libraries
import os
import requests
from requests.auth import HTTPDigestAuth
import json
import pandas as pd
import cx_Oracle
import configparser
from sqlalchemy import types, create_engine
import time
import numpy as np
from datetime import datetime, timedelta
import pandasql as ps
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

try:
    config = configparser.ConfigParser()
    config.read('config.ini')
   
    #DB data
    user=config['DB']['user']
    passwd=config['DB']['passwd']
    host = config['DB']['host']
    port = config['DB']['port']
    SID = config['DB']['SID']
    
    #Payload data
    username=config['Payload']['username']
    password=config['Payload']['password']
    client_id=config['Payload']['client_id']
    client_secret=config['Payload']['client_secret']

    # Creating an engine and a connection to connect to the databse
    engine = create_engine(f"oracle+cx_oracle://{user}:{passwd}@{host}:{port}/?service_name={SID}.cisco.com", max_identifier_length=128, max_overflow=30)
    conn_oracle = engine.connect()
    print(conn_oracle)

    url = "https://cloudsso.cisco.com/as/token.oauth2"

    payload = f"grant_type=password&username={username}&password={password}&client_id={client_id}&client_secret={client_secret}"
    headers = {
    'content-type': "application/x-www-form-urlencoded",
    'cache-control': "no-cache"
    }

    response = requests.request("POST", url, data=payload, headers=headers, timeout=15)
    print(response.text)
    r = response.json()

    #Step4: Get the authentication token after sign in
    token = r.get('access_token')
    print(token)

    #code to fetch data from database
    conn_string = f"{user}/{passwd}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SID={SID})))"
    conn = cx_Oracle.connect(conn_string)
    cursor = conn.cursor()
    print("Connected Successfully")

    sql_statement="select * from PROD_DATA_COLLECTION_TIMESTAMP where ID = 5"
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    for i in range(len(records)):
        records[i] = list(records[i])
    df_time = pd.DataFrame(records,columns=['ID', 'TABLE_NAME', 'UPDATED_AT', 'TIME_ZONE'])
    print('Fetched Product data from Database')
    time = df_time['UPDATED_AT'][0]
    date = time.split(" ")[0]
    print('Time :',date)

    url = "https://tpsapiservices.cisco.com/tps/metrics/products?page=0&size=500&sort=id,asc&startTime="+str(date)+"T00%3A00%3A00.000Z"

    headers = {
    'authorization': "Bearer " + token,
    'cache-control': "no-cache",
    }

    response = requests.request("GET", url, headers=headers)
    j = response.json()
    total_counts = j['totalElements']
    total_pages = j['totalPages']
    print("Total number of elements returned :",total_counts)
    print("Total number of pages :",total_pages)

    list_products = []
    for i in range(0, total_pages):
        print(i)
        url = "https://tpsapiservices.cisco.com/tps/metrics/products?page="+str(i)+"&size=500&sort=id,asc&startTime="+str(date)+"T00%3A00%3A00.000Z"
        response = requests.request("GET", url, headers=headers)
        j = response.json()
        #list_components.append(j['content'])
        for a in j['content']:
            list_products.append(a)
        #i = i + 1
    products = pd.DataFrame(list_products)
    products.head()
    products_empty = products.empty
    if products_empty == False:
        products.rename(columns={'updatedAt':'UPDATED_AT'}, inplace=True)
        products.rename(columns={'createdAt':'CREATED_AT'}, inplace=True)
        products.rename(columns={'coronaProductId':'CORONA_PRODUCT_ID'}, inplace=True)
        products.rename(columns={'deleted':'DELETED'}, inplace=True)
        products.rename(columns={'name':'NAME'}, inplace=True)
        products.rename(columns={'id':'ID'}, inplace=True)

        products = products[[
            'ID', 'NAME', 'CREATED_AT', 'UPDATED_AT', 'DELETED', 'CORONA_PRODUCT_ID'
        ]]

        products['NAME'] = products['NAME'].str.encode(encoding='utf-8')
        products['NAME'] = products['NAME'].astype(str)
        products['NAME'] = products['NAME'].str.replace("b'", "")
        products['NAME'] = products['NAME'].str.replace("'", "")
        products['NAME'] = products['NAME'].astype(str)
        products = products.replace('None', np.nan)
        sql_statement="select * from PROD_PRODUCT"
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        for i in range(len(records)):
            records[i] = list(records[i])
        df_extract = pd.DataFrame(records,columns=['ID', 'NAME', 'CREATED_AT', 'UPDATED_AT', 'DELETED', 'CORONA_PRODUCT_ID'])
        print('Fetched Product data from Database')

        #code to compare data in database and the data coming in from API(s)
        #---------------------------------------------------------------------------------------------------------------
        #Code to find rows which are not already in database table
        q1 = """SELECT * FROM products where ID NOT IN (select ID from df_extract) """
        not_in_db = ps.sqldf(q1)

        #Code to find rows which are already in database table
        q2 = """SELECT * FROM products where ID IN (select ID from df_extract) """
        in_db = ps.sqldf(q2)
        #---------------------------------------------------------------------------------------------------------------

        condition_1 = in_db.empty
        if condition_1 == False:
            print('No, data needs to be updated')
            #to create tuples of the data
            data = [tuple(x) for x in in_db[['ID', 'NAME', 'CREATED_AT', 'UPDATED_AT', 'DELETED', 'CORONA_PRODUCT_ID']].values]
            #to update the data in the database table
            update_sql = "update prod_product set id=:id, name=:name, created_at=:created_at, updated_at=:updated_at, deleted=:deleted, corona_product_id=:corona_product_id where id =:id"
            cursor.executemany(update_sql, data)
            conn.commit()
            print('Updated the data in database table')
        condition_2 = not_in_db.empty
        if condition_2 == False:
            print('No, data is not in database needs to be added')
            #Code to push data into database 
            dtyp = {c:types.VARCHAR(not_in_db[c].str.len().max())
                    for c in not_in_db.columns[not_in_db.dtypes == 'object'].tolist()}
            #have to write to push data into database
            not_in_db.to_sql('prod_product', conn_oracle, schema='qauser1', if_exists='append', index=False, dtype=dtyp)
            print('Success: Appended data in Prod Components table in Data-lake')

        sql_statement="""select max(to_char(to_timestamp(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"'),'YYYY-MM-DD HH24:MI:SS')) from prod_Product"""
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        for i in range(len(records)):
            records[i] = list(records[i])
        df_time = pd.DataFrame(records,columns=['TIME'])
        print('Fetched Product data from Database')

        data2 = df_time['TIME'][0]
        print('Last updated timestamp :', data2)
        update_sql_1 = "update prod_data_collection_timestamp set updated_at =:data2 where id = 5"
        cursor.execute(update_sql_1, {"data2" :str(data2)})
        conn.commit()
        print('Updated the date in database table')
        print('Job finished, no new data has been added')
        cursor.close()
        print('Cursor Closed')
        conn.close() 
        print('Connection Closed')
        print('Job finished, new data added/updated')

    else:
        sql_statement="""select max(to_char(to_timestamp(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"'),'YYYY-MM-DD HH24:MI:SS')) from prod_Product"""
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        for i in range(len(records)):
            records[i] = list(records[i])
        df_time = pd.DataFrame(records,columns=['TIME'])
        print('Fetched Product data from Database')

        data2 = df_time['TIME'][0]
        print('Last updated timestamp :', data2)
        update_sql_1 = "update prod_data_collection_timestamp set updated_at =:data2 where id = 5"
        cursor.execute(update_sql_1, {"data2" :str(data2)})
        conn.commit()
        cursor.close()
        print('Cursor Closed')
        conn.close() 
        print('Connection Closed')
        print('Job finished, new data added/updated')
        print('Updated the date in database table')
        print('Job finished, no new data has been added')

except Exception as e:
    print(e)
    #Step5:Sending an email
    server = smtplib.SMTP('outbound.cisco.com', 25)
    server.ehlo()
    server.starttls()
    html = """Hello, \n
    Error in AWS Release Script.\n
    \n
    \n
    \n
    Thanks,\n 
    TPSD Team""" 
    msg = MIMEMultipart()
    emails = list(config['Email']['id'].split(","))
    cc = list(config['Email']['cc'].split(","))
    msg['From'] = 'metrics-noreply'
    msg['To'] = ', '.join(emails)
    msg['Cc'] = ', '.join(cc)
    msg['Subject'] = 'ERROR IN API PRODUCT SCRIPT'
    try:
        print("Email Sent Successfully")
    except:
        print("failed")
    server.quit()
