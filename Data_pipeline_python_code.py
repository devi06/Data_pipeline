"""
Created on Thu Jul 18 22:41:34 2019

@author: laksh
"""
#pip install cx_Oracle
#pip install sqlalchemy
import sqlalchemy as db
import cx_Oracle
engine = db.create_engine('oracle://<username>:<pass>@oracle10.cgsyjtny2rf9.us-east-2.rds.amazonaws.com:1521/orcl')

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
    s3 = boto3.client('s3', aws_access_key_id='<accessKey>', aws_secret_access_key='<secretkey>')
    s3.upload_file(table+'.csv', 'shubhanandya', table+'.csv')
