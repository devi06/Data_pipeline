"""
Created on Thu Jul 18 22:41:34 2019

@author: laksh
"""

import sqlalchemy as db
import cx_Oracle
engine = db.create_engine('oracle://USER10:USER10@oracle10.cgsyjtny2rf9.us-east-2.rds.amazonaws.com:1521/orcl')

import pandas as pd
tables_df = pd.read_sql_query("select TABLE_NAME from all_tables WHERE OWNER='OT'",engine.connect())

for colname,tablename in tables_df.items():
    tab = tablename

for table in tab:
    df = pd.read_sql_query("select * from ot."+table,engine.connect())
    import pyarrow as pa
    df.to_csv(table+'.csv',index=False) 
    #import pyarrow as pd
    #df.to_csv('warehouses.csv',index=False)  
    import boto3
    s3 = boto3.client('s3', aws_access_key_id='AKIA6ERB5MROQWZOIFSW', aws_secret_access_key='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')
    s3.upload_file(table+'.csv', 'shubhanandya', table+'.csv')