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
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

def main(sch_id):
        # import pdb
        # pdb.set_trace()

        current_date=timezone.now().date()
        date = timezone.now()
        mssg_pipeline_schedule_t='validation sucessfull in pipeline schedule'
        mssg_pipeline_detail_t='validation sucessfull in pipeline detail'
        mssg_pipeline_t='validation sucessfull in pipeline '
        mssg_congig_t='validation sucessfull in pipeline configuration'
        mssg_connection_detail_source_t='validation sucessfull in connection detail source'
        mssg_connection_detail_target_t='validation sucessfull in connection detail target'
        mssg_conection_source_t='validation sucessfull in connection source'
        mssg_connection_target_t='validation sucessfull in connection target'

        mssg_pipeline_schedule_f='validation failed in pipeline schedule'
        mssg_pipeline_detail_f='validation failed in pipeline detail'
        mssg_pipeline_f='validation failed in pipeline '
        mssg_congig_f='validation failed in pipeline configuration'
        mssg_connection_detail_source_f='validation failed in connection detail source'
        mssg_connection_detail_target_f='validation failed in connection detail target'
        mssg_conection_source_f='validation failed in connection source'
        mssg_connection_target_f='validation failed in connection target'
       

        schedules_id=pipeline_schedule.objects.filter(id=sch_id).values()
        temp_sid=schedules_id[0]

        pipeline_id=pipeline_details.objects.filter(id=temp_sid['pipeline_det_id_id']).values()
        temp_pipe=pipeline_id[0]

        config=pipeline.objects.filter(id=temp_pipe['pipeline_id_id']).values()
        temp_config=config[0]

        connection_detail_ids=db_config.objects.filter(id=temp_config['config_id_id']).values()
        temp_connection=connection_detail_ids[0]

        source_connection=connection_detail.objects.filter(id=temp_connection['Source_conn_det_id_id']).values()
        temp_source=source_connection[0]

        target_connection=connection_detail.objects.filter(id=temp_connection['Target_conn_det_id_id']).values()
        temp_target=target_connection[0]

        connection_id=conn.objects.filter(id=temp_source['connection_id_id']).values()
        temp_souconnection_name=connection_id[0]

        connection_id=conn.objects.filter(id=temp_target['connection_id_id']).values()
        temp_tarconnection_name=connection_id[0]

        print(schedules_id)

        post_value_slog=schedule_log(schedule_id=temp_sid['id'],pipeline_id=temp_pipe['pipeline_id_id'],status='running')

        print(post_value_slog)

        post_value_slog.save() 

        run_id= schedule_log.objects.filter(schedule_id=temp_sid['id']).values('run_id')
        temp_rid=run_id[0]
        print(run_id)

        


        if temp_sid['id']>= 1 and temp_sid['pipeline_status'] == 1 and temp_sid['pipeline_schedule_start_date']<=current_date and temp_sid['pipeline_schedule_end_date']>=current_date :
                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_schedule_t)
                audit_los_save.save()
                if temp_pipe['id']>= 1 and temp_pipe['status'] == 1 and temp_pipe['start_date']<=current_date and temp_pipe['end_date']>=current_date :
                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_detail_t)
                        audit_los_save.save()
                        if temp_config['id']>= 1 and temp_config['is_active'] == 1 and temp_config['Start_date']<=current_date and temp_config['End_date']>=current_date :
                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_t)
                                audit_los_save.save()
                                if temp_connection['id']>= 1 and temp_connection['is_active'] == 1 and temp_connection['start_date']<=current_date and temp_connection['end_date']>=current_date :
                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_congig_t)
                                        audit_los_save.save()
                                        if temp_source['id']>= 1 and temp_source['is_active'] == 1 and temp_source['start_date']<=current_date and temp_source['end_date']>=current_date :
                                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_connection_detail_source_t)
                                                audit_los_save.save()
                                                if temp_target['id']>= 1 and temp_target['is_active'] == 1 and temp_target['start_date']<=current_date and temp_target['end_date']>=current_date :
                                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_connection_detail_target_t)
                                                        audit_los_save.save()
                                                        if temp_souconnection_name['id']>= 1 and temp_souconnection_name['is_active'] == 1 and temp_souconnection_name['start_date']<=current_date and temp_souconnection_name['end_date']>=current_date :
                                                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_conection_source_t)
                                                                audit_los_save.save()
                                                                if temp_tarconnection_name['id']>= 1 and temp_tarconnection_name['is_active'] == 1 and temp_tarconnection_name['start_date']<=current_date and temp_tarconnection_name['end_date']>=current_date :
                                                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_connection_target_t)
                                                                        audit_los_save.save()
                
                                                                        dataset=[]
                                                                       
                                                                        target={}
                                                                        sources={}
                                                                        sources.update({"source_connection_id":temp_souconnection_name['id']})
                                                                        sources.update(temp_source['con_str'])
                                                                        sources.update({"source_table_name":temp_pipe['source_table_name']})
                                                                        sources.update({"run_id":temp_rid['run_id']})
                                                                        sources.update({"target_connection_id":temp_tarconnection_name['id']})
                                                                        #target.update({"schedue_id":temp_rid['run_id']})
                                                                        target.update({"target_connection_id":temp_tarconnection_name['id']})
                                                                        target.update(temp_target['con_str'])
                                                                        target.update({"target_table_name":temp_pipe['target_table_name']})
                                                                        target.update({"run_id":temp_rid['run_id'] })
                                                                        dataset.append(sources)
                                                                        dataset.append(target)
                                                                        print(sources)
                                                                        print(target)
                                                                        print(dataset)
                                                                        wraper_scripts(dataset=dataset)
                                                                        return (dataset)
                                                                        
                                                                else:
                                                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_connection_target_f)
                                                                        audit_los_save.save()
                                                                        error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=1,status=mssg_connection_target_f)
                                                                        error_log_save.save()
                                                                        put(sch_id)
                                                        else:
                                                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_conection_source_f)
                                                                audit_los_save.save()
                                                                error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_conection_source_f)
                                                                error_log_save.save()
                                                                put(sch_id)
                                                else:
                                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_connection_detail_target_f)
                                                        audit_los_save.save()
                                                        error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_connection_detail_target_f)
                                                        error_log_save.save()
                                                        put(sch_id)
                                        else:
                                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_connection_detail_source_f)
                                                audit_los_save.save()
                                                error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_connection_detail_source_f)
                                                error_log_save.save()
                                                put(sch_id)
                                else:
                                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_congig_f)
                                        audit_los_save.save()
                                        error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_congig_f)
                                        error_log_save.save()
                                        put(sch_id)
                        else:
                                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_f)
                                audit_los_save.save()
                                error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_f)
                                error_log_save.save()
                                put(sch_id)
                else:
                        audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_detail_f)
                        audit_los_save.save()
                        error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_detail_f)
                        error_log_save.save()
                        put(sch_id)
        else:
                audit_los_save=audit_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_schedule_f)
                audit_los_save.save()
                error_log_save=error_log(run_id=temp_rid['run_id'],schedule_id=sch_id,status=mssg_pipeline_schedule_f)
                error_log_save.save()
                put(sch_id)
        

def put(sch_id):
        qset=schedule_log.objects.get(schedule_id=sch_id)
        qset.status=("failled")
        qset.save()
        return Response(qset)

def put_c(sch_id):
        qset=schedule_log.objects.get(schedule_id=sch_id)
        qset.status=("complited")
        qset.save()
        return Response(qset)

def wraper_scripts(dataset):
        print(dataset)
        sources=dataset[0]
        target=dataset[1]
        print(sources)
        print(target)
        data_set=source_mapping(sources=sources)
        print("hii iam reurning set")
        print(data_set)
        target_mapping(target=target,data_set=data_set)
        run_ids=sources['run_id']
        put_c(sch_id=run_ids)
def source_mapping(sources):
        print(sources)
        source_connection_id=sources['source_connection_id']
        if source_connection_id == 2:
                data_set=source_postgres(sources=sources)
        elif source_connection_id == 1:
                data_set=source_sf(sources=sources)
        elif source_connection_id == 3:
                data_set=source_mysql(sources=sources)
        elif source_connection_id == 4:
                data_set=source_aws(sources=sources)
        elif source_connection_id == 5:
                data_set=source_oracle(sources=sources)
        elif source_connection_id == 6:
                data_set=source_teradata(sources=sources)
        elif source_connection_id == 7:
                data_set=source_sql_lite(sources=sources)
        elif source_connection_id==8:
                data_set=read_csv()
        return data_set
        

def target_mapping(target,data_set):
        print(target)
        target_connection_id=target['target_connection_id']
        if target_connection_id == 1:
                target_sf(target=target,data_set=data_set)                
        elif target_connection_id == 2:
                target_postgres(target=target,data_set=data_set)
        elif target_connection_id == 3:
                target_mysql(target=target,data_set=data_set)
        elif target_connection_id == 4:
                target_oracle(target=target,data_set=data_set)
        elif target_connection_id == 5:
                target_teradata(target=target,data_set=data_set)
        elif target_connection_id == 6:
                target_sql_lite(target=target,data_set=data_set)
                                                                        
# def source_postgres(sources):
#         try:

#                 # establishing the connection
#                 conn = ps.connect(
#                         database=sources['database'],
#                 user=sources['user'],
#                 password=sources['password'],
#                 host=sources['host'],
#                 port= sources['port']
#                 )
#                 cur = conn.cursor()


#                 sql = f"select * from {sources['source_table_name']}"
#                 cur.execute(sql)
#                 df = pd.DataFrame(cur)
#                 print('read the table' ,df)
#         except (Exception, ps.DatabaseError) as error:
#                 print(error)
#         return df
def source_postgres(sources):
        try:
        # establishing the connection

                conn = ps.connect(
                database=sources['database'],
                user=sources['user'],
                password=sources['password'],
                host=sources['host'],
                port= sources['port']
                 )
                cur = conn.cursor()
                sql = f"select * from {sources['source_table_name']}"
                sql2 = f"select * from {sources['source_table_name']} limit 10"
                cur.execute(sql)
                df2 = pd.read_sql_query(sql2,conn)
                # Fetch data and column names
                column_names = [desc[0].upper() for desc in cur.description]
                data = cur.fetchall()
                print('Read completed..')
                 # Create a list of arrays, one for each column
                arrays = [pa.array(column) for column in zip(*data)]
                # Create a Table from the arrays and column names
                table = pa.Table.from_arrays(arrays, column_names)
                # Write the table to a Parquet file
                pq.write_table(table,'gcp_url', compression='snappy')
                print('parquet file generated..')
                gcp_url = 'gcs://data4423/'
                path_to_private_key = r'/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json'
                client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
                bucket = client.bucket('data4423')
                # t_name = sources['source_table_name']
                blob = bucket.blob(f"{sources['source_table_name']}.parquet")      
                blob.upload_from_filename('gcp_url')
                print('file loaded to GCS..')

        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df2


def target_postgres(target,data_set):
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


def source_mysql(sources):
        try:

                conn = msql.connect(host=sources['host'],
                                        user=sources['user'],
                                        password=sources['password'],
                                        port=sources['port'],
                                        database=sources['database']
                                        )
                cur = conn.cursor()

                sql = f"select * from {sources['source_table_name']}"
                sql2 = f"select * from {sources['source_table_name']} limit 10"
                cur.execute(sql)
                df = pd.read_sql_query(sql2,conn)
                # Fetch data and column names
                column_names = [desc[0].upper() for desc in cur.description]
                data = cur.fetchall()
                print('Read completed..')
                 # Create a list of arrays, one for each column
                arrays = [pa.array(column) for column in zip(*data)]
                # Create a Table from the arrays and column names
                table = pa.Table.from_arrays(arrays, column_names)
                # Write the table to a Parquet file
                pq.write_table(table,'gcp_url', compression='snappy')
                print('parquet file generated..')
                gcp_url = 'gcs://data4423/'
                path_to_private_key = r'/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json'
                client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
                bucket = client.bucket('data4423')
                # t_name = sources['source_table_name']
                blob = bucket.blob(f"{sources['source_table_name']}.parquet")      
                blob.upload_from_filename('gcp_url')
                print('file loaded to GCS..')

        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_mysql(target,data_set): 
        conn = msql.connect(host=target['host'],
                                user=target['user'],
                                password=target['password'],
                                port=target['port'],
                                database=target['database']
                                )
        cur = conn.cursor()

        df2 = pd.DataFrame(data_set, index=None, columns=None)
#     print(df2.values)
#     column=df2.columns.values
#     dtype=df2.dtypes.values
#     create_statement=create_tbl(column=column,dtype=dtype)
#     remove_brace=str(create_statement)[1:-1]
#     final_create_statement=remove_brace.replace("'","")
#     sql =f'''CREATE TABLE {target['table_name']}({final_create_statement})'''
#     cur.execute(sql)
#     conn.commit()
#     print("Table created successfully........")
        for tups in tuple(df2.values.tolist()):

                sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                cur.execute(sql2)
                conn.commit()

def source_sql_lite(sources):
        try:
                # create con object to connect
                # the database geeks_db.db
                conn = sqlite3.connect("mydb.db")
                cur = conn.cursor()

                sql = f"select * from {sources['source_table_name']}"
                sql2 = f"select * from {sources['source_table_name']} limit 10"
                cur.execute(sql)
                df = pd.read_sql_query(sql2,conn)
                # Fetch data and column names
                column_names = [desc[0].upper() for desc in cur.description]
                data = cur.fetchall()
                print('Read completed..')
                # Create a list of arrays, one for each column
                arrays = [pa.array(column) for column in zip(*data)]
                # Create a Table from the arrays and column names
                table = pa.Table.from_arrays(arrays, column_names)
                # Write the table to a Parquet file
                pq.write_table(table,'gcp_url', compression='snappy')
                print('parquet file generated..')
                gcp_url = 'gcs://data4423/'
                path_to_private_key = r'/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json'
                client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
                bucket = client.bucket('data4423')
                # t_name = sources['source_table_name']
                blob = bucket.blob(f"{sources['source_table_name']}.parquet")      
                blob.upload_from_filename('gcp_url')
                print('file loaded to GCS..')
                print('read the table' ,df)

        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_sql_lite(data_set,target):
                conn = sqlite3.connect("mydb.db")
                cur = conn.cursor()
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                # print(df2.values)
                # column=df2.columns.values
                # dtype=df2.dtypes.values
                # create_statement=create_tbl(column=column,dtype=dtype)
                # remove_brace=str(create_statement)[1:-1]
                # final_create_statement=remove_brace.replace("'","")
                # sql =f'''CREATE TABLE {target['table_name']}({final_create_statement})'''
                # cur.execute(sql)
                # conn.commit()
                # print("Table created successfully........")
                for tups in tuple(df2.values.tolist()):

                        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                cur.execute(sql2)
                conn.commit()

def source_oracle(sources):
        try:

                connstr =f"{sources['user']}/{sources['password']}@{sources['host']}:{sources['port']}/{sources['database']}"

                conn = cx_Oracle.connect(connstr)
                cur = conn.cursor()
                print("connected")


                sql = f"select * from {sources['source_table_name']}"
                sql2 = f"select * from {sources['source_table_name']} limit 10"
                cur.execute(sql)
                df = pd.read_sql_query(sql2,conn)
                # Fetch data and column names
                column_names = [desc[0].upper() for desc in cur.description]
                data = cur.fetchall()
                print('Read completed..')
                 # Create a list of arrays, one for each column
                arrays = [pa.array(column) for column in zip(*data)]
                # Create a Table from the arrays and column names
                table = pa.Table.from_arrays(arrays, column_names)
                # Write the table to a Parquet file
                pq.write_table(table,'gcp_url', compression='snappy')
                print('parquet file generated..')
                gcp_url = 'gcs://databucket8806/'
                path_to_private_key = r'/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json'
                client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
                bucket = client.bucket('databucket8806')
                blob = bucket.blob(f"{sources['source_table_name']}.parquet")      
                blob.upload_from_filename('gcp_url')
                print('file loaded to GCS..')
                print('read the table' ,df)

        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_oracle(data_set,target):

                connstr = f"{target['user']}/{target['password']}@{target['host']}:{target['port']}/{target['database']}"

                conn = cx_Oracle.connect(connstr)
                cur = conn.cursor()
                print("connected")
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                # print(df2.values)
                # column=df2.columns.values
                # dtype=df2.dtypes.values
                # create_statement=create_tbl(column=column,dtype=dtype)
                # remove_brace=str(create_statement)[1:-1]
                # final_create_statement=remove_brace.replace("'","")
                # sql =f'''CREATE TABLE {target['table_name']}({final_create_statement})'''
                # cur.execute(sql)
                # conn.commit()
                # print("Table created successfully........")
                for tups in tuple(df2.values.tolist()):
                        print(tuple(tups))

                sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

                cur.execute(sql2)
                conn.commit()



def source_aws(source):
                client = boto3.client(
                's3',
                aws_access_key_id=source['access_key'],
                aws_secret_access_key=source['secret_key'],
                region_name=source['region_name']
                )
                print('connected')
                clientResponse = client.list_buckets()
                for bucket in clientResponse['Buckets']:
                        print(f'Bucket Name: {bucket["Name"]}')

                        obj = client.get_object(
                                Bucket=source['Bucket'],
                                Key=source['Key']
                        )
                        # Read data from the S3 object
                        data = pd.read_csv(obj['Body'])
                        # Print the data frame
                        print('Printing the data frame...')
                        print(data)
                        return data

def source_sf(sources):
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
                role='ACCOUNTADMIN'
                # insecure_mode=True,
                )
                print(sources)
                
                print("Connection successful")
                cur=conn.cursor()
                sql = f"select * from {sources['source_table_name']}"
                sql2 = f"select * from {sources['source_table_name']} limit 10"
                cur.execute(sql)
                df = pd.read_sql_query(sql2,conn)
                # Fetch data and column names
                column_names = [desc[0].upper() for desc in cur.description]
                data = cur.fetchall()
                print('Read completed..')
                 # Create a list of arrays, one for each column
                arrays = [pa.array(column) for column in zip(*data)]
                # Create a Table from the arrays and column names
                table = pa.Table.from_arrays(arrays, column_names)
                # Write the table to a Parquet file
                pq.write_table(table,'gcp_url', compression='snappy')
                print('parquet file generated..')
                gcp_url = 'gcs://databucket8806/'
                path_to_private_key = r'/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json'
                client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
                bucket = client.bucket('data4423')
                blob = bucket.blob(f"{sources['source_table_name']}.parquet")      
                blob.upload_from_filename('gcp_url')
                print('file loaded to GCS..')
                print('read the table' ,df)

        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df




def target_sf(data_set,target):
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
                df2 = pd.DataFrame(data_set, index=None, columns=None)
                print(df2.values)
                column=df2.columns.values
                dtype=df2.dtypes.values
                create_statement=create_tbl(column=column,dtype=dtype)
                remove_brace=str(create_statement)[1:-1]
                final_create_statement=remove_brace.replace("'","")
                sql =f'''CREATE OR REPLACE TABLE {target['target_table_name']}({final_create_statement})'''
                cur.execute(sql)
                url.commit()
                print("Table created successfully........")

                print("sf connected")
                cur.execute("""CREATE or replace STORAGE INTEGRATION gcs_inter
                        TYPE = EXTERNAL_STAGE
                        STORAGE_PROVIDER = 'GCS'
                        ENABLED = TRUE
                        STORAGE_ALLOWED_LOCATIONS = ('gcs://data4423/');""")

                cur.execute("""create or replace file format My_parquet
                        type = 'parquet'""")

                cur.execute(f"""create or replace stage my_gcs_stage
                        url = "gcs://data4423/t_name.parquet"
                        storage_integration = gcs_inter
                        FILE_FORMAT = (format_name = My_parquet);""")

                cur.execute(f"""
                        copy into {target['target_table_name']} from @my_gcs_stage
                        FILE_FORMAT=(format_name = My_parquet)
                        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
                        on_error = 'skip_file';""")

                print('data loaded to table..')
                cur.execute("drop stage my_gcs_stage")
                print('stage dropped..')

                url.commit()
                # Closing the connection
        except (Exception, pyodbc.DatabaseError) as error:
                print(error)



def source_teradata(sources):
        try:
                url = teradatasql.connect(
                        host=sources['server'],
                        user=sources['username'],
                        password=sources['password'],
                        encryptdata='true',
                )
                cur = url.cursor()
                print("teradata connected")
                sql = f"select * from {sources['source_table_name']}"
                sql2 = f"select * from {sources['source_table_name']} limit 10"
                cur.execute(sql)
                df = pd.read_sql_query(sql2,conn)
                # Fetch data and column names
                column_names = [desc[0].upper() for desc in cur.description]
                data = cur.fetchall()
                print('Read completed..')
                 # Create a list of arrays, one for each column
                arrays = [pa.array(column) for column in zip(*data)]
                # Create a Table from the arrays and column names
                table = pa.Table.from_arrays(arrays, column_names)
                # Write the table to a Parquet file
                pq.write_table(table,'gcp_url', compression='snappy')
                print('parquet file generated..')
                gcp_url = 'gcs://data4423/'
                path_to_private_key = r'/home/ubuntu/datahub-v3/datahub_v3_project/gcp.json'
                client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
                bucket = client.bucket('data4423')
                t_name = sources['source_table_name']
                blob = bucket.blob(f"{sources['source_table_name']}.parquet")      
                blob.upload_from_filename('gcp_url')
                print('file loaded to GCS..')

        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df

def target_teradata(data_set,target):
    url = teradatasql.connect(
        host=target['server'],
        user=target['username'],
        password=target['password'],
        encryptdata='true',
    )
    cur = url.cursor()
    print("teradata connected")
    df2 = pd.DataFrame(data_set, index=None, columns=None)
#     print(df2.values)
#     column=df2.columns.values
#     dtype=df2.dtypes.values
#     create_statement=create_tbl(column=column,dtype=dtype)
#     remove_brace=str(create_statement)[1:-1]
#     final_create_statement=remove_brace.replace("'","")
#     sql =f'''CREATE TABLE {target['table_name']}({final_create_statement})'''
#     cur.execute(sql)
#     url.commit()
#     print("Table created successfully........")

     
    for tups in tuple(df2.values.tolist()):
        print(tuple(tups))

        sql2 = f"insert into {target['target_table_name']} values {tuple(tups)}"

        cur.execute(sql2)
        url.commit()


def read_csv(sources):
        try:

                df = pd.read_csv(fr"{sources['file']}")
                print(df)
        except (Exception, ps.DatabaseError) as error:
                print(error)
        return df


def getColumnDtypes(dataTypes):
#     import pdb
#     pdb.set_trace()
                        
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar')
    return dataList
    
def create_tbl(column,dtype):
    dty=getColumnDtypes(dataTypes=dtype)
    set=[]
    for i ,j in zip(column,dty):
        print(i+' '+j+',')
        dummy=i+' '+j
        set.append(dummy)
    return set
