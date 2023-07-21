#new
import django
django.setup()
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'datahub_v3_project.settings')
from pipeline_details_api.serializers import *
from datahub_v3_app.models import pipeline_details
from django.shortcuts import render
from rest_framework.views import APIView
#from.models import pipe_line
#from .serializers import pipe_lineserializer
from django.http.response import Http404
from urllib import response
import ast
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
from functools import partial
import numpy as np
import io
from email_frm.views import *
from multiprocessing import Pool



def schema_fram(schema_id):
        import pdb
        pdb.set_trace()
        print(schema_id)
        current_date=timezone.now().date()
        mssg_schema_mig_t='validation sucessfull in schema migration'
        mssg_congig_t='validation sucessfull in pipeline configuration'
        mssg_connection_detail_source_t='validation sucessfull in connection detail source'
        mssg_connection_detail_target_t='validation sucessfull in connection detail target'
        mssg_conection_source_t='validation sucessfull in connection source'
        mssg_connection_target_t='validation sucessfull in connection target'

        mssg_schema_mig_t='validation failed in schema migration'
        mssg_congig_f='validation failed in pipeline configuration'
        mssg_connection_detail_source_f='validation failed in connection detail source'
        mssg_connection_detail_target_f='validation failed in connection detail target'
        mssg_conection_source_f='validation failed in connection source'
        mssg_connection_target_f='validation failed in connection target'
        
        today = date.today() 
        time=datetime.datetime.now()
        migraton_set=schema_migration.objects.filter(id=schema_id).values()
        temp_migration={}
        for i in migraton_set:
                temp_migration.update(i)
        
        config_set=db_config.objects.filter(id=temp_migration['config_id_id']).values()
        temp_config={}
        for i in config_set:
                temp_config.update(i)

        source_connection=connection_detail.objects.filter(id=temp_config['Source_conn_det_id_id']).values()
        temp_source={}
        for i in source_connection:
                temp_source.update(i)

        target_connection=connection_detail.objects.filter(id=temp_config['Target_conn_det_id_id']).values()
        temp_target={}
        for i in target_connection:
                temp_target.update(i)

        source_connection_id=conn.objects.filter(id=temp_source['connection_id_id']).values()
        temp_souconnection_name={}
        for i in source_connection_id:
                temp_souconnection_name.update(i)

        target_connection_id=conn.objects.filter(id=temp_target['connection_id_id']).values()
        temp_tarconnection_name={}
        for i in target_connection_id:
                temp_tarconnection_name.update(i)
        # schedule id posting
        post_schema= schema_log(config_id=temp_config['id'],schema_name=temp_migration['schema_name'],config_name=temp_config['config_name'],schema_id=temp_migration['id'],status="running",start_date=today,start_time=time,)        
        post_schema.save()
        # getting run id

        run_id= schema_log.objects.filter(schema_id=temp_migration['id']).values('run_id')
        temp_rid={}
        for i in run_id:
                temp_rid.update(i)
        print(run_id)
        run_ids=temp_rid['run_id']

        import pdb
        pdb.set_trace()
       
        if temp_migration['id']>= 1 and temp_migration['is_active'] == 1 and temp_migration['start_date']<=current_date and temp_migration['end_date']>=current_date :
                audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_schema_mig_t)
                audit_los_save.save()
                if temp_config['id']>= 1 and temp_config['is_active'] == 1 and temp_config['start_date']<=current_date and temp_config['end_date']>=current_date :
                        audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_congig_t)
                        audit_los_save.save()
                        if temp_source['id']>= 1 and temp_source['is_active'] == 1 and temp_source['start_date']<=current_date and temp_source['end_date']>=current_date :
                                audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_source_t)
                                audit_los_save.save()
                                if temp_target['id']>= 1 and temp_target['is_active'] == 1 and temp_target['start_date']<=current_date and temp_target['end_date']>=current_date :
                                        audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_target_t)
                                        audit_los_save.save()
                                        if temp_souconnection_name['id']>= 1 and temp_souconnection_name['is_active'] == 1 and temp_souconnection_name['start_date']<=current_date and temp_souconnection_name['end_date']>=current_date :
                                                audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_conection_source_t)
                                                audit_los_save.save()
                                                if temp_tarconnection_name['id']>= 1 and temp_tarconnection_name['is_active'] == 1 and temp_tarconnection_name['start_date']<=current_date and temp_tarconnection_name['end_date']>=current_date :
                                                        audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_target_t)
                                                        audit_los_save.save()

                                                        dataset=[]
                                                        target={}
                                                        sources={}
                                                        sources.update({"source_connection_id":temp_souconnection_name['id']})
                                                        sources.update(temp_source['con_str'])
                                                        sources.update({"run_id":temp_rid['run_id']})
                                                        sources.update({"source_table_name":temp_migration['schema_name']})
                                                        #target.update({"target_table_name":temp_pipe['target_table_name']})
                                                        sources.update({"table_list":temp_migration['source_table_name']})
                                                        sources.update({"all_table":temp_migration['all_tables']})
                                                        sources.update({"email":temp_migration['email']})
                                                        sources.update({"migration_id":schema_id})
                                                        sources.update({"target_connection_id":temp_tarconnection_name['id']})
                                                        target.update({"target_connection_id":temp_tarconnection_name['id']})
                                                        target.update(temp_target['con_str'])
                                                        target.update({"target_table_name":temp_migration['schema_name']})
                                                        target.update({"run_id":temp_rid['run_id']})
                                                        target.update({"migration_id":schema_id})
                                                
                                                        dataset.append(sources)
                                                        dataset.append(target)
                                                        print(sources)
                                                        print(target)
                                                        print(dataset)
                                                        wraper_script(dataset=dataset)
                                                        
                                                        return (dataset)
                                                else:
                                                        audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_target_f)
                                                        audit_los_save.save()
                                                        error_log_save=schema_error_log(run_id=temp_rid['run_id'],schedule_id=1,status=mssg_connection_target_f)
                                                        error_log_save.save()
                                                        put(run_ids)
                                        else:
                                                audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_conection_source_f)
                                                audit_los_save.save()
                                                error_log_save=schema_error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_conection_source_f)
                                                error_log_save.save()
                                                put(run_ids)
                                else:
                                        audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_target_f)
                                        audit_los_save.save()
                                        error_log_save=schema_error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_target_f)
                                        error_log_save.save()
                                        put(run_ids)
                        else:
                                audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_source_f)
                                audit_los_save.save()
                                error_log_save=schema_error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_connection_detail_source_f)
                                error_log_save.save()
                                put(run_ids)
                else:
                        audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_congig_f)
                        audit_los_save.save()
                        error_log_save=schema_error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_congig_f)
                        error_log_save.save()
                        put(run_ids)
        else:
                audit_los_save=schema_audit_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_schema_mig_t)
                audit_los_save.save()
                error_log_save=schema_error_log(run_id=temp_rid['run_id'],schedule_id=schema_id,status=mssg_schema_mig_t)
                error_log_save.save()
                put(run_ids)
def create_table(dataset):
        # import pdb
        # pdb.set_trace()
        sources=dataset[1]
        target=dataset[0]
        result = migration_log.objects.values('table_name').filter(schema_migration_id=53).annotate(dcount=Count('column_name'))
        print(result)
        conn = snowflake.connector.connect(
                user=sources['user'],
                password=sources['password'],
                account=sources['account'],
                warehouse=sources['warehouse'],
                database=sources['database'],
                schema=sources['schema'],
                role=sources['role']
                )
        cur=conn.cursor()
        print('connecton succes')
        for i in result:
                table=i['table_name']
                tbl=migration_log.objects.filter(table_name=table).values('table_name','column_name','data_type')
                print(tbl)
                create_statement=create_tbl(i=tbl)
                remove_brace=str(create_statement)[1:-1]
                final_create_statement=remove_brace.replace("'","")
                sql =f'''CREATE TABLE {table}({final_create_statement})'''
                print(sql)
                cur.execute(sql)
                conn.commit()
        return'hii'


               
def put(run_ids):
        time=datetime.datetime.now()
        today = date.today()
        qset=schema_log.objects.get(run_id=run_ids)
        qset.status=("failed")
        qset.end_time=time
        qset.end_date=today
        qset.save()
        return Response(qset)


def put_c(run_ids):
        import pdb
        pdb.set_trace()
        time=datetime.datetime.now()
        today = date.today()
        qset2=schema_log.objects.filter(run_id=run_ids).values()
        qset=schema_log.objects.get(run_id=run_ids)
        qset.status=("completed")
        qset.end_time=time
        qset.end_date=today
        qset.save()
        return Response(qset)

def put_play(mig_id):
        qset=schema_migration.objects.get(run_id=mig_id)
        qset.Play=(False)
        qset.save()
        return Response(qset)

def schema_mapping(sources):
        import pdb
        pdb.set_trace()
        print(sources)
        source_connection_id=sources['source_connection_id']
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
           #     import pdb
            #    pdb.set_trace()
                table_name=schema_sqlserver(sources=sources)
        elif source_connection_id == 7:
                table_name=source_sql_lite(sources=sources)
        
        return table_name

def create_table_wraper(target):
        print(target)
        target_connection_id=target['target_connection_id']
        if target_connection_id == 1:
         #       import pdb
          #      pdb.set_trace()
                create_tbl_sf(target=target)  
        elif target_connection_id == 2:
                create_tbl_pg(target=target)
        elif target_connection_id == 3:
                create_tbl_msql(target=target)
        elif target_connection_id == 4:
                create_tbl_ora(target=target)
        elif target_connection_id == 5:
                create_tbl_tera(target=target)
        elif target_connection_id == 6:
                create_tbl_sqlserver(target=target)


def wraper_script(dataset):
        import pdb
        pdb.set_trace()
        print(dataset)
        sources=dataset[0]
        target=dataset[1]
        print(sources)
        print(target)
        #import pdb
        #pdb.set_trace()
        
        table_name=schema_mapping(sources=sources)
        create_table_wraper(target=target)
        result = migration_log.objects.values('table_name').filter(schema_migration_id=sources['migration_id']).annotate(dcount=Count('column_name'))
        
        print(result)
        table_names=[]
        for i in result:
                table_names.append(i['table_name'])
        print(table_names)    
        
        with Pool(processes=len(table_names)) as pool:
                pool.map(source_mapping, [(sources, table_name) for table_name in table_names])

        # Map target_mapping() function to table_names using the Pool
        with Pool(processes=len(table_names)) as pool:
                pool.map(target_mapping, [(target, table_name) for table_name in table_names])
        run_ids = sources['run_id']
        mig_id = sources['migration_id']
        send_email = sources['email']

        subject = "Migration Status"
        body = "Your Migration has been done, Sucessfully"
        email_send(send_email,bod=body,sub=subject)
       # put_c(run_ids)
        put_play(mig_id)
        


def source_mapping(args):
        sources, table_name = args
        print(sources)
        source_connection_id=sources['source_connection_id']
        if source_connection_id == 1:
                data_set=source_sf(sources=sources, table_name=table_name)
        elif source_connection_id == 2:
                data_set=source_postgres(sources=sources, table_name=table_name)
        elif source_connection_id == 3:
                data_set=source_mysql(sources=sources, table_name=table_name)
        elif source_connection_id == 4:
                data_set=source_oracle(sources=sources, table_name=table_name)
        elif source_connection_id == 5:
                data_set=source_teradata(sources=sources, table_name=table_name)
        elif source_connection_id == 6:
                data_set=source_sqlserver(sources=sources, table_name=table_name)
        return data_set
        

def target_mapping(args):
        target, table_name = args
        print(target)
        target_connection_id=target['target_connection_id']
        if target_connection_id == 1:
                target_sf(target=target,table_name=table_name)  
        elif target_connection_id == 2:
                target_postgres(target=target,data_set=data_set)
        elif target_connection_id == 3:
                target_mysql(target=target,data_set=data_set)
        elif target_connection_id == 4:
                target_oracle(target=target,data_set=data_set)
        elif target_connection_id == 5:
                target_teradata(target=target,data_set=data_set)
        elif target_connection_id == 6:
                target_sqlserver(target=target,table_name=table_name)


#schema mapping
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
                if sources['all_table']:
                        for i in df.values.tolist():
                                schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['source_table_name'],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0],row_count=100)
                                schema_save.save()
                else:

                        for i in df.values.tolist():
                                schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['source_table_name'],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0],row_count=100)
                                schema_save.save()
                df = pd.DataFrame(cur)
                
                print(df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def schema_sf(sources):
        try:
                import pdb
                pdb.set_trace()
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
                list=sources['table_list']
                if sources['all_table']:
                        for i in df.values.tolist():
                                print(i)
                                schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                schema_save.save()
                #table_names=df.values.tolist()
                else:

                        for i in df[df['TABLE_NAME'].isin(list)].values.tolist():
                                print(i)
                                schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                schema_save.save()
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return 'end'
def schema_mssql(sources):
        try:
                import pdb
                pdb.set_trace()
                print('connecting..')
                scnn = msql.connect(host=sources['host'],
                                port=sources['port'],
                                user=sources['user'],
                                password=sources['password'],
                                database=sources['database']
                                )
                cs = scnn.cursor()
               # table={'name':'DATAHUB2'}
                sql= f"SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{table['name']}';  "
                cs.execute(sql)
                print(cs)
                df=pd.read_sql_query(sql,conn)
                if sources['all_table']:
                        for i in df.values.tolist():
                                schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                schema_save.save()
                else:
                        for i in df[df['TABLE_NAME'].isin(list)].values.tolist():
                                schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                schema_save.save()
                print(df)
                print('completed..')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def schema_sqlserver(sources):

        try:

                #import pdb
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
                lisst=sources['table_list']
                sql = "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,ORDINAL_POSITION,TABLE_SCHEMA FROM information_schema.COLUMNS;"
                df=pd.read_sql_query(sql,conn)
                if sources['all_table']:
                        for i in df.values.tolist():
                                schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=i[4],row_count=100,column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                schema_save.save()
                else:
                        for i in df[df['TABLE_NAME'].isin(lisst)].values.tolist():
                                        schema_save=migration_log(schema_migration_id=sources['migration_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=i[4],row_count=100,column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                        schema_save.save()
                print(df)

        except (Exception, pyodbc.DatabaseError) as error:
                print(error)            

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
                if sources['all_table']:
                        for i in df.values.tolist():
                                schema_save=migration_log(schedule_id=sources['schedule_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                schema_save.save()
                
                else:

                        for i in df[df['TABLE_NAME'].isin(list)].values.tolist():
                                schema_save=migration_log(schedule_id=sources['schedule_id'],source_connection_id=sources['source_connection_id'],target_connection_id=sources['target_connection_id'],source_schema_name=sources['schema'],row_count=i[4],column_name=i[1],data_type=i[2],original_position=i[3],table_name=i[0])
                                schema_save.save()
                print(type(df))
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df


def create_tbl_sf(target):
        try:
                import pdb
                pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                conn = snowflake.connector.connect(
                        user=target['user'],
                        password=target['password'],
                        account=target['account'],
                        warehouse=target['warehouse'],
                        database=target['database'],
                        schema=target['schema'],
                        role=target['role']
                        )
                cur=conn.cursor()
                print('connecton success')
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE OR REPLACE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
                        print('Table created Successfully'+ table)
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_pg(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                conn = ps.connect(
                        database=target['database'],
                        user=target['user'],
                        password=target['password'],
                        host=target['host'],
                        port= target['port']
                )
                cur = conn.cursor()
                print('connecton success')
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
                conn.close()
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_msql(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                
                conn = msql.connect(host=target['host'],
                                        user=target['user'],
                                        password=target['password'],
                                        port=target['port'],
                                        database=target['database']
                                        )
                cur = conn.cursor()
                print('connecton success')
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
                conn.close()
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_ora(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                connstr =f"{target['user']}/{target['password']}@{target['host']}:{target['port']}/{target['database']}"

                conn = cx_Oracle.connect(connstr)
                cur = connstr.cursor()
                print("connected")

                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_tera(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                conn = teradatasql.connect(
                        host=target['server'],
                        user=target['username'],
                        password=target['password'],
                        encryptdata='true',
                )
                cur = conn.cursor()
                print("teradata connected")
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
def create_tbl_sqlserver(target):
        try:
                # import pdb
                # pdb.set_trace()
                
                result = migration_log.objects.values('table_name').filter(schema_migration_id=target['migration_id']).annotate(dcount=Count('column_name'))
                print(result)
                conn = pyodbc.connect(DRIVER=target['driver'],
                SERVER=target['server'],
                PORT=target['port'],
                DATABASE=target['database'],
                UID=target['user'],
                PWD=target['password'])
                cur = conn.cursor()
                print("teradata connected")
                for i in result:
                        table=i['table_name']
                        tbl=migration_log.objects.filter(table_name=table).filter(schema_migration_id=target['migration_id']).values('table_name','column_name','data_type')
                        print(tbl)
                        create_statement=create_tbl_sqlserver(i=tbl)
                        remove_brace=str(create_statement)[1:-1]
                        final_create_statement=remove_brace.replace("'","")
                        sql =f'''CREATE TABLE {table}({final_create_statement})'''
                        print(sql)
                        cur.execute(sql)
                        conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
                                                                        
def source_postgres(sources, table_name):
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

                query = f"SELECT * FROM {table_name}"
                cur.execute(query)
                column_names = [desc[0].upper() for desc in cur.description]
                chunksize = 250000
                offset = 0
                # Fetch the data in chunks
                while True:
                        data = cur.fetchmany(chunksize)
                        if not data:
                                break
                        arrays = [pa.array(column) for column in zip(*data)]
                        table = pa.Table.from_arrays(arrays, column_names)
                        parquet_file = io.BytesIO()
                        # gcp_url = 'gcs://data4423/'
                        pq.write_table(table, parquet_file, compression='snappy')
                        parquet_file.seek(0)
                        # print(gcp_url)
                        # Use BufferReader instead of io.BytesIO to read the Parquet file
                        reader = pa.BufferReader(parquet_file.getbuffer())
                        json_credentials_path = r"/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json"
                        client = storage.Client.from_service_account_json(json_credentials_path=json_credentials_path)
                        bucket = client.bucket("databucket8806")
                        blob = bucket.blob(f"{table_name}_{offset}.parquet")
                        # blob.upload_from_filename('gcp_url')
                        blob.upload_from_file(reader, content_type='application/octet-stream')
                        print(f'Upload complete for gcs')
                        offset += chunksize
                print(f'All data uploaded for {table_name} ')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return "done"
      


def target_postgres(target,data_set):
        try:
                # establishing the connection
                conn = ps.connect(
                        database=target['database'],
                        user=target['user'],
                        password=target['password'],
                        host=target['host'],
                        port=target['port']
                )
                cur = conn.cursor()

                df2 = pd.DataFrame(data_set, index=None, columns=None)
                print(df2.values)
                
                for tups in tuple(df2.values.tolist()):

                        print(tuple(tups))

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                        cur.execute(sql2)
                        conn.commit()

        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df2


def source_mysql(sources, table_name):
        try:

                conn = msql.connect(host=sources['host'],
                                user=sources['user'],
                                password=sources['password'],
                                port=sources['port'],
                                database=sources['database']
                                        )
                cur = conn.cursor()

                query = f"SELECT * FROM {table_name}"
                cur.execute(query)
                column_names = [desc[0].upper() for desc in cur.description]
                chunksize = 250000
                offset = 0
                # Fetch the data in chunks
                while True:
                        data = cur.fetchmany(chunksize)
                        if not data:
                                break
                        arrays = [pa.array(column) for column in zip(*data)]
                        table = pa.Table.from_arrays(arrays, column_names)
                        parquet_file = io.BytesIO()
                        # gcp_url = 'gcs://data4423/'
                        pq.write_table(table, parquet_file, compression='snappy')
                        parquet_file.seek(0)
                        # print(gcp_url)
                        # Use BufferReader instead of io.BytesIO to read the Parquet file
                        reader = pa.BufferReader(parquet_file.getbuffer())
                        json_credentials_path = r"/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json"
                        client = storage.Client.from_service_account_json(json_credentials_path=json_credentials_path)
                        bucket = client.bucket("databucket8806")
                        blob = bucket.blob(f"{table_name}_{offset}.parquet")
                        # blob.upload_from_filename('gcp_url')
                        blob.upload_from_file(reader, content_type='application/octet-stream')
                        print(f'Upload complete for gcs')
                        offset += chunksize
                print(f'All data uploaded for {table_name} ')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_mysql(target,data_set):
        try:

                conn = msql.connect(host=target['host'],
                                        user=target['user'],
                                        password=target['password'],
                                        port=target['port'],
                                        database=target['database']
                                        )
                cur = conn.cursor()

                df2 = pd.DataFrame(data_set, index=None, columns=None)
                
                for tups in tuple(df2.values.tolist()):

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                        cur.execute(sql2)
                        conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)

        return df2

def source_sql_lite(sources, table_name):
        try:

                # create con object to connect
                # the database geeks_db.db
                conn = sqlite3.connect("mydb.db")
                cur = conn.cursor()


                sql = f"SELECT * from {sources['source_table_name']}"
                cur.execute(sql)
                df = pd.DataFrame(cur)
                print('read the table', df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_sql_lite(data_set,target):
        try:


                conn = sqlite3.connect("mydb.db")
                cur = conn.cursor()
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                
                for tups in tuple(df2.values.tolist()):

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                cur.execute(sql2)
                conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
        
        return df2

def source_oracle(sources, table_name):
        try:

                connstr =f"{sources['user']}/{sources['password']}@{sources['host']}:{sources['port']}/{sources['database']}"

                conn = cx_Oracle.connect(connstr)
                cur = conn.cursor()
                print("connected")


                sql = f"select * from {sources['source_table_name']}"
                cur.execute(sql)
                df = pd.DataFrame(cur)
                print('read the table', df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_oracle(data_set,target):
        try:

                connstr = f"{target['user']}/{target['password']}@{target['host']}:{target['port']}/{target['database']}"

                conn = cx_Oracle.connect(connstr)
                cur = conn.cursor()
                print("connected")
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                print(df2.values)
                
                for tups in tuple(df2.values.tolist()):
                        print(tuple(tups))

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                cur.execute(sql2)
                conn.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)
        
        return df2


def source_sf(sources, table_name):
        print(sources)
        try:
                # import pdb
                # pdb.set_trace()
                print('Connecting to te sf database...')
                conn = snowflake.connector.connect(
                user=sources['user'],
                password=sources['password'],
                account=sources['account'],
                warehouse=sources['warehouse'],
                database=sources['database'],
                schema=sources['schema'],
                role='ACCOUNTADMIN',
                insecure_mode=True,
                )
                
                print("Connection successful")
                cur=conn.cursor()
                query = f"SELECT * FROM {table_name}"
                cur.execute(query)
                column_names = [desc[0].upper() for desc in cur.description]
                chunksize = 250000
                offset = 0
                # Fetch the data in chunks
                while True:
                        data = cur.fetchmany(chunksize)
                        if not data:
                                break
                        arrays = [pa.array(column) for column in zip(*data)]
                        table = pa.Table.from_arrays(arrays, column_names)
                        parquet_file = io.BytesIO()
                        # gcp_url = 'gcs://data4423/'
                        pq.write_table(table, parquet_file, compression='snappy')
                        parquet_file.seek(0)
                        # print(gcp_url)
                        # Use BufferReader instead of io.BytesIO to read the Parquet file
                        reader = pa.BufferReader(parquet_file.getbuffer())
                        json_credentials_path = r"/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json"
                        client = storage.Client.from_service_account_json(json_credentials_path=json_credentials_path)
                        bucket = client.bucket("databucket8806")
                        blob = bucket.blob(f"{table_name}_{offset}.parquet")
                        # blob.upload_from_filename('gcp_url')
                        blob.upload_from_file(reader, content_type='application/octet-stream')
                        print(f'Upload complete for gcs')
                        offset += chunksize
                print(f'All data uploaded for {table_name} ')
        except (Exception, ps.DatabaseError) as error:
                print(error)
                id=sources['run_id']
                put(run_ids=id)
        return "hi"



def target_sf(target, table_name):
        import pdb
        pdb.set_trace()
        try:
                url = snowflake.connector.connect(
                        user=target['user'],
                        password=target['password'],
                        account=target['account'],
                        warehouse=target['warehouse'],
                        database=target['database'],
                        schema=target['schema'],
                        role='ACCOUNTADMIN'
                )
                
                cur = url.cursor()
                # import pdb
                # pdb.set_trace()
                print("sf connected")
                # print(table_name)
                # temp_tbl=np.array(table_name)
                # list = ast.literal_eval(table_name)
                temp_tbl=[]
                temp_tbl.append(table_name)
                print(temp_tbl)
                for i in temp_tbl:
                        print(i)
                        print(f'Migrating data from sf {i}...')
                        cur = url.cursor()
                        cur.execute("""CREATE or replace STORAGE INTEGRATION gcs_inter
                                        TYPE = EXTERNAL_STAGE
                                        STORAGE_PROVIDER = 'GCS'
                                        ENABLED = TRUE
                                        STORAGE_ALLOWED_LOCATIONS = ('gcs://databucket8806/');""")
                        chunksize = 250000
                        offset = 0
                        # Iterate over chunks and increase offset
                        while True:
                                j = offset // chunksize
                                format_name = f"{i}format{j}"
                                cur.execute(f"""CREATE OR REPLACE FILE FORMAT {format_name}
                                                                        TYPE = 'PARQUET'""")

                                # Create a stage for the current chunk
                                chunk_stage_name = f"{i}chunk{j}"
                                url = f'gcs://databucket8806/{i}_{j * chunksize}.parquet'
                                cur.execute(f"""CREATE OR REPLACE STAGE {chunk_stage_name}
                                                                        URL = '{url}'
                                                                        storage_integration = gcs_inter
                                                                        FILE_FORMAT = (FORMAT_NAME = {format_name})""")

                                # Load data from the current stage into the table
                                cur.execute(f"""COPY INTO {i} FROM @{chunk_stage_name}
                                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                                        on_error = 'skip_file';""")

                                # Drop the file format and stage after data is loaded
                                cur.execute(f"DROP FILE FORMAT {format_name}")
                                cur.execute(f"DROP STAGE {chunk_stage_name}")
                                print(f'data loaded to {i} for offset {j}')

                                # Check if there is more data
                                json_credentials_path = r"/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json"
                                client = storage.Client.from_service_account_json(json_credentials_path=json_credentials_path)
                                bucket = client.bucket("databucket8806")
                                blobs = bucket.list_blobs(prefix=f"{i}_")
                                if len(list(blobs)) == j + 1:
                                        break
                                # Increment the offset
                                offset += chunksize

                        print(f"All data for table {i} is uploaded.")

        except (Exception, pyodbc.DatabaseError) as error:
                print(error)
                id=target['run_id']
                put(run_ids=id)


def source_teradata(sources, table_name):
        try:
                print(f'Migrating data from {table_name}')
                url = teradatasql.connect(
                        host=sources['server'],
                        user=sources['username'],
                        password=sources['password'],
                        encryptdata='true',
                )
                cur = url.cursor()
                print("teradata connected")
                sql = f"select * from {sources['database']}.{table_name}"
                cur.execute(sql)
                column_names = [desc[0].upper() for desc in cur.description]
                # chunksize = 250000
                # offset = 0
                # Fetch the data in chunks
                while True:
                        data = cur.fetchmany(chunksize)
                        if not data:
                                break
                        arrays = [pa.array(column) for column in zip(*data)]
                        table = pa.Table.from_arrays(arrays, column_names)
                        parquet_file = io.BytesIO()
                        pq.write_table(table, parquet_file, compression='snappy')
                        parquet_file.seek(0)

                        # Use BufferReader instead of io.BytesIO to read the Parquet file
                        reader = pa.BufferReader(parquet_file.getbuffer())
                        json_credentials_path = r"/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json"
                        client = storage.Client.from_service_account_json(json_credentials_path=json_credentials_path)
                        bucket = client.bucket("databucket8806")
                        blob = bucket.blob(f"{table_name}_{offset}.parquet")
                        blob.upload_from_file(reader, content_type='application/octet-stream')
                        print(f'Upload complete for gcs {table_name}_{offset}')
                        offset += chunksize
                print(f'All data uploaded for table {table_name}')
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_teradata(data_set,target):
        try:

                url = teradatasql.connect(
                        host=target['server'],
                        user=target['username'],
                        password=target['password'],
                        encryptdata='true',
                )
                cur = url.cursor()
                print("teradata connected")
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                print(df2.values)

                
                for tups in tuple(df2.values.tolist()):
                        print(tuple(tups))

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                        cur.execute(sql2)
                        url.commit()
        except (Exception, ps.DatabaseError) as error:
                print(error)

def source_sqlserver(sources, table_name):
        try:
                import pdb
                pdb.set_trace()
                print(f'Migrating data from {table_name}')
                url = pyodbc.connect(
                DRIVER=sources['driver'],
                SERVER=sources['server'],
                PORT=sources['port'],
                DATABASE=sources['database'],
                UID=sources['username'],
                PWD=sources['password']
                )
                cur = url.cursor()
                query = f"SELECT * FROM {table_name}"
                cur.execute(query)
                column_names = [desc[0].upper() for desc in cur.description]
                chunksize = 25000
                offset = 0
                # Fetch the data in chunks
                while True:
                        data = cur.fetchmany(chunksize)
                        if not data:
                                break
                        arrays = [pa.array(column) for column in zip(*data)]
                        table = pa.Table.from_arrays(arrays, column_names)
                        parquet_file = io.BytesIO()
                        # gcp_url = 'gcs://data4423/'
                        pq.write_table(table, parquet_file, compression='snappy')
                        parquet_file.seek(0)
                        # print(gcp_url)
                        # Use BufferReader instead of io.BytesIO to read the Parquet file
                        reader = pa.BufferReader(parquet_file.getbuffer())
                        json_credentials_path = r"/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json"
                        client = storage.Client.from_service_account_json(json_credentials_path=json_credentials_path)
                        bucket = client.bucket("databucket8806")
                        blob = bucket.blob(f"{table_name}_{offset}.parquet")
                        # blob.upload_from_filename('gcp_url')
                        blob.upload_from_file(reader, content_type='application/octet-stream')
                        print(f'Upload complete for gcs')
                        offset += chunksize
                print(f'All data uploaded for {table_name} ')

        except (Exception, pyodbc.DatabaseError) as error:
                print(error)
                id=sources['run_id']
                put(run_ids=id)
        return "hi"


def target_sqlserver(target,data_set):
        try:
                # import pdb
                # pdb.set_trace()
                print('Connecting to the PostgresSQL database...')
                url = pyodbc.connect(DRIVER=target['driver'],
                                SERVER=target['server'],
                                PORT=target['port'],
                                DATABASE=target['database'],
                                UID=target['username'],
                                PWD=target['password'])
                print("Connection successful")

                cur = url.cursor()
                df2 = pd.DataFrame(data_set, index=None, columns=None)

                for tups in tuple(df2.values.tolist()):
                                print(tuple(tups))

                                sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                                cur.execute(sql2)
                                url.commit()
                # Closing the connection
        except (Exception, pyodbc.DatabaseError) as error:
                print(error)
                id=target['run_id']
                put(run_ids=id)
def getColumnDtypes(dataTypes):
#     import pdb
#     pdb.set_trace()
                        
    dataList = []
    for x in dataTypes:
        if(x == 'integer'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar')
    return dataList
    
def create_tbl(i):
        import pdb
        pdb.set_trace()
        data_ty=[]  
        column=[]
        for s in i:
                column.append(s['column_name'])
                data_ty.append(s['data_type'])
        # print(column['column_name'])
        # print(column['data_type'])
        print('hi')
        print('hlo')
        dty=getColumnDtypes(data_ty)
        set=[]
        for i ,j in zip(column,dty):
                print(i+' '+j+',')
                dummy=i+' '+j
                set.append(dummy)
        return set

def getColumnDtypes_sqlserver(dataTypes):
#     import pdb
#     pdb.set_trace()
                        
    dataList = []
    for x in dataTypes:
        if (x == 'bigint'):
            dataList.append('int')
        elif (x == 'float(255)'):
            dataList.append('float(255)')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar(500)')
    return dataList
    
def create_tbl_sqlserver(i):
        import pdb
        pdb.set_trace()
        data_ty=[]  
        column=[]
        for s in i:
                column.append(s['column_name'])
                data_ty.append(s['data_type'])
        # print(column['column_name'])
        # print(column['data_type'])
        print('hi')
        print('hlo')
        dty=getColumnDtypes(data_ty)
        set=[]
        for i ,j in zip(column,dty):
                print(i+' '+j+',')
                dummy=i+' '+j
                set.append(dummy)
        return set
