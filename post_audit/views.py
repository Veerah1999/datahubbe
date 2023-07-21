from django.shortcuts import render
from rest_framework.views import APIView
#from.models import pipe_line
#from .serializers import pipe_lineserializer
from django.http.response import Http404
from urllib import response
from rest_framework.response import Response
from pipeline_details_api.serializers import *
from django.db.models import Count
import json
import requests
import pyodbc
from datahub_v3_app.models import *
from pipeline_framework.serializers import *
from pipeline_schedule_api.serializers import *
from django.utils import timezone
import pandas as pd
import snowflake.connector as sf
import snowflake.connector
import mysql.connector as msql
from snowflake.connector.pandas_tools import write_pandas
import psycopg2 as ps
import teradatasql
import boto3
import cx_Oracle
import json
import sqlite3
import csv
import datetime
from datetime import date
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq

class sample_p(APIView):

    def get(self,request,pk):
       # import pdb
       # pdb.set_trace()
        schema_temp=schema_migration.objects.filter(id=pk).values()
        schema={}
        for i in schema_temp:
            schema.update(i)
        config_temp=db_config.objects.filter(id=schema['config_id_id']).values()
        config={}
        for i in config_temp:
            config.update(i)
        conection_det_temp_t=connection_detail.objects.filter(id=config['Target_conn_det_id_id']).values()
        conn_tar={}
        for i in conection_det_temp_t:
            conn_tar.update(i)
        sources={}
        sources.update({'connection_id_id':conn_tar['connection_id_id']})
        sources.update(conn_tar['con_str'])
        t=post_mapping(sources)
        return Response(t)
       
def post_mapping(sources):
       # import pdb
       # pdb.set_trace()
        print(sources)
        source_connection_id=sources['connection_id_id']
        if source_connection_id == 1:
                table_name=schema_sf(sources=sources)
        elif source_connection_id == 2:
                table_name=schema_pg(sources=sources)
        elif source_connection_id == 3:
                table_name=schema_mssql(sources=sources)
        elif source_connection_id == 4:
                table_name=source_teradata(sources=sources)
        elif source_connection_id == 5:
                table_name=source_teradata(sources=sources)
        elif source_connection_id == 6:
                table_name=schema_sqlserver(sources=sources)
        # elif source_connection_id == 7:
                # table_name=source_sql_lite(sources=sources)
        
        return table_name
def schema_pg(sources):
    try:
       # import pdb
       # pdb.set_trace()

        # establishing the connection
        conn = ps.connect(
                database=sources['database'],
                user=sources['user'],
                password=sources['password'],
                host=sources['host'],
                port= sources['port']
        )
        cur = conn.cursor()
        # import pdb
        # pdb.set_trace()

        sql='''select table_name,column_name,data_type,ordinal_position  from information_schema.columns where table_schema='public'order by table_name,columns.ordinal_position;'''
        #cur2.execute(database)
        df=pd.read_sql_query(sql,conn)
        obj=df.to_dict('records')
    except (Exception, ps.DatabaseError) as error:
            print(error)
    return obj

def schema_sf(sources):
        try:
                print('Connecting to te sf database...')
                conn = snowflake.connector.connect(
                user=sources['user'],
                password=sources['password'],
                account=sources['account'],
                warehouse=sources['warehouse'],
                database=sources['database'],
                schema=sources['schema'],
                role=sources['role']
                )
                print("Connection successful")
                print('hi')
                #print
                cur = conn.cursor()
                
                sql='''select distinct a.table_name,a.column_name,a.data_type,a.ordinal_position,b.row_count from information_schema.columns
                a join information_schema.tables b on a.table_name=b.table_name where table_type = 'BASE TABLE' order by a.table_name,a.ordinal_position ;'''
                #cur2.execute(database)
                df=pd.read_sql_query(sql,conn)
                obj=df.to_dict('records')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return obj
def schema_mssql(sources):
        try:

                print('connecting..')
                scnn = msql.connect(host=sources['host'],
                                port=sources['port'],
                                user=sources['user'],
                                password=sources['password'],
                                database=sources['database']
                                )
                cs = scnn.cursor()
                table={'name':'DATAHUB2'}
                sql= f"SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{table['name']}';  "
                cs.execute(sql)
                df=pd.read_sql_query(sql,conn)
                obj=df.to_dict('records')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return obj

def schema_sqlserver(sources):

        try:

            # import pdb
            # pdb.set_trace()
            print('Connecting to the PostgresSQL database...')
            conn = pyodbc.connect(DRIVER=sources['driver'],
                            SERVER=sources['server'],
                            PORT=sources['port'],
                            DATABASE=sources['database'],
                            UID=sources['username'],
                            PWD=sources['password'])
            print("Connection successful")
            cur = conn.cursor()
            sql = "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,ORDINAL_POSITION,TABLE_SCHEMA FROM information_schema.COLUMNS;"
            df=pd.read_sql_query(sql,conn)
            obj=df.to_dict('records')
            print(df)
        except (Exception, pyodbc.DatabaseError) as error:
                print(error)
        return obj
        
        

def source_teradata(sources):

        try:
                # import pdb
                # pdb.set_trace()
                conn = teradatasql.connect(
                        host=sources['server'],
                        user=sources['username'],
                        password=sources['password'],
                        encryptdata='true',
                )
                cur=conn.cursor()
                databases=sources['database']
                print("Connection successful")
                sql = """select TableName,DataBaseName,ColumnName,ColumnLength,ColumnType,ColumnId,CreatorName 
                from columns where databasename={databases}"""
                df=pd.read_sql_query(sql,conn)
                obj=df.to_dict('records')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return obj
