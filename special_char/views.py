import sqlite3
from django.http.response import Http404
from datahub_v3_app.models import *
from rest_framework.views import APIView
from special_char.serializers import *
from rest_framework.response import Response
import pandas as pd
import snowflake.connector
import mysql.connector
import psycopg2 as ps
import cx_Oracle
import pyodbc
import teradatasql


class special_char(APIView):
    def get(self, request, pk=None):
            if pk:
                 data = spl_columnchange.objects.get(pk=pk)
                 var_serializer = spchar_serializer(data)
                 return Response(var_serializer.data)
            else:
                data = spl_columnchange.objects.all()
                var_serializer = spchar_serializer(data, many=True)
                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        serializer = spchar_serializer(data=data)

        serializer.is_valid(raise_exception=True)

        serializer.save()
        
        response = Response()

        response.data = {
            'message': 'Created Successfully',
            'data': serializer.data
        }
        return response


    def put(self, request, pk=None, format=None):
        old_data = spl_columnchange.objects.get(pk=pk)
        var_serializer = spchar_serializer(instance=old_data,data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'Updated Successfully',
            'data': var_serializer.data
        }

        return response
    def delete(self, request, pk, format=None):
        delete_data =  spl_columnchange.objects.get(pk=pk)

        delete_data.delete()

        return Response({
            'message': 'Deleted Successfully'
        })
    

class schema_det(APIView):

    def get(self,request,pk):
        # import pdb
        # pdb.set_trace()
        # pipe_det_temp = pipeline_details.objects.filter(id = pk).values()
        # pipe_det = {}
        # for i in pipe_det_temp:
        #       pipe_det.update(i)
        # pipeline_temp = pipeline.objects.filter(id=pipe_det['pipeline_id_id']).values()
        # pipeline_dict = {}
        # for i in pipeline_temp:
        #       pipeline_dict.update(i)
        # config_temp = db_config.objects.filter(id =pipeline_dict['config_id_id']).values()
        # config = {}
        # for i in config_temp:
        #      config.update(i)
        soucondet_temp = connection_detail.objects.filter(id = pk).values()
        soucondet ={}
        for i in soucondet_temp:
             soucondet.update(i)
        print(soucondet)
        sources = {}
        sources.update({'source_connection_id':soucondet['connection_id_id']})
        sources.update(soucondet['con_str'])
        sources.update({'table_name': 'testdb' })
        trigger = wrap_all(sources=sources)
        return Response(trigger)


def wrap_all(sources):
    source_connection_id=sources['source_connection_id']
    if source_connection_id == 1:
            table_name=schema_for_sf(sources=sources)
    elif source_connection_id == 2:
            table_name=schema_for_pg(sources=sources)
    elif source_connection_id == 3:
            table_name=schema_for_mysql(sources=sources)
    elif source_connection_id == 4:
            table_name=schema_for_oracle(sources=sources)
    elif source_connection_id == 5:
            table_name=schema_for_teradata(sources=sources)
    elif source_connection_id == 6:
            table_name=schema_for_sqlserver(sources=sources)
    elif source_connection_id == 7:
            table_name=schema_for_sql_lite(sources=sources)
    return table_name
         
         
# def schema_for_pg(sources):
#     try: 
#         conn = ps.connect(
#                             database=sources['database'],
#                             user=sources['user'],
#                             password=sources['password'],
#                             host=sources['host'],
#                             port= sources['port']
#                     )
#         cur = conn.cursor()

#         query = f'''select table_name,column_name,data_type,ordinal_position  from information_schema.columns where table_schema='public'order by table_name,columns.ordinal_position;'''           
#         df = pd.read_sql_query(query,conn)
#         s = df.to_dict('records')


#     except (Exception, ps.DatabaseError) as error:
#         print(error)
#     return s
def schema_for_pg(sources):
    s = []  # Assign an initial value to s
    try: 
        conn = ps.connect(
            database=sources['database'],
            user=sources['user'],
            password=sources['password'],
            host=sources['host'],
            port=sources['port']
        )
        cur = conn.cursor()

        query = f'''select table_name,column_name,data_type,ordinal_position  
                    from information_schema.columns where table_schema='public' 
                    order by table_name,columns.ordinal_position;'''
        df = pd.read_sql_query(query, conn)
        s = df.to_dict('records')

    except (Exception, ps.DatabaseError) as error:
        print(error)

    return s

def schema_for_sf(sources):
    try:
        conn = snowflake.connector.connect(
                    user=sources['user'],
                    password=sources['password'],
                    account=sources['account'],
                    warehouse=sources['warehouse'],
                    database=sources['database'],
                    schema=sources['schema'],
                    role=sources['role']
                    )
        cur = conn.cursor()

        sql='''select distinct a.table_name,a.column_name,a.data_type,a.ordinal_position,b.row_count from information_schema.columns
                a join information_schema.tables b on a.table_name=b.table_name where table_type = 'BASE TABLE' order by a.table_name,a.ordinal_position ;'''
        df = pd.read_sql_query(sql,conn)
        s = df.to_dict('records')
        print(s)
    except (Exception, ps.DatabaseError) as error:
        print(error)
    return s


def schema_for_mysql(sources):
    try:
      
        conn = mysql.connect(host=sources['host'],
                                    port=sources['port'],
                                    user=sources['user'],
                                    password=sources['password'],
                                    database=sources['database']
                                    )
        cur = conn.cursor()
        sql= f"SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='';  "
        df = pd.read_sql_query(sql,conn)
        

    except (Exception, ps.DatabaseError) as error:
                print(error)
    return df
def schema_for_oracle(sources):
    try:
          conn = f"{sources['user']}/{sources['password']}@{sources['host']}:{sources['port']}/{sources['database']}"
          conn = cx_Oracle.connect(conn)
          cur = conn.cursor()
          print("connected")
          sql=f'''SELECT t.table_name, c.column_name, c.data_type, c.column_id, t.num_rows FROM all_tables t JOIN all_tab_columns c ON t.table_name = c.table_name AND t.owner = c.owner WHERE t.owner = 'DATAHUB' ORDER BY t.table_name, c.column_id'''


          df = pd.read_sql_query(sql, conn)
          s = df.to_dict('records')


    except (Exception, ps.DatabaseError) as error:
        print(error)
    return s
    
def schema_for_sqlserver(sources):
    try:
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
        s = df.to_dict('records')


    except (Exception, ps.DatabaseError) as error:
        print(error)
    return s

def schema_for_teradata(sources):
    try:
        conn = teradatasql.connect(
                    host=sources['server'],
                    user=sources['username'],
                    password=sources['password'],
                    encryptdata='true',
            )
        cur=conn.cursor()
        databases=sources['database']
        print("Connection successful")
        sql = f"Select * from {sources['table_name']}"
        df=pd.read_sql_query(sql,conn)
        s = df.to_dict('records')


    except (Exception, ps.DatabaseError) as error:
        print(error)
    return s
      
def schema_for_sql_lite(sources):
    try:
        conn = sqlite3.connect("mydb.db")
        cur = conn.cursor()
        sql = f"SELECT * from {sources['table_name']}"
        df=pd.read_sql_query(sql,conn)
        s = df.to_dict('records')

    except (Exception, ps.DatabaseError) as error:
        print(error)
    return s

      
    

    

# cursor = url.cursor()
#         sql=f"use {database}"
#         cursor.execute(sql)
#         print("connected")
#         script = """
#    select a.TABLE_NAME , a.TABLE_SCHEMA, a.TABLE_CATALOG,jim.BYTES,jim.ROW_COUNT from snowflake.account_usage.tables a 
#     left join INFORMATION_SCHEMA.tables jim on (a.TABLE_NAME = jim.TABLE_NAME) ;
#     """
#         cursor.execute(script)
#         columns = [desc[0] for desc in cursor.description]
#         data = cursor.fetchall()
#         df = pd.DataFrame(list(data), columns=columns)
#         df3 = df.fillna(0)
#         df1 = df3.astype({'BYTES': int, 'ROW_COUNT': int})
#         script2= '''     
#   select  a.TABLE_NAME , a.TABLE_SCHEMA, a.TABLE_CATALOG,b.COLUMN_NAME ,b.DATA_TYPE,b.ORDINAL_POSITION from 
#   snowflake.account_usage.tables a left join INFORMATION_SCHEMA.columns b on (b.TABLE_NAME = a.TABLE_NAME) ;'''
#         cursor.execute(script2)
#         columns = [desc[0] for desc in cursor.description]
#         data = cursor.fetchall()
#         df2 = pd.DataFrame(list(data), columns=columns,index=None)
#         # dummy1 = getColumnDtypes(df2['DATA_TYPE'].tolist())
#         s = df1.to_dict('records')
#         print(s)
#         #obj = [{'id': x[0], 'table': x[1],'schema':x[2],'database':x[3],'table_size':x[4],'row_count':x[5]} for x in s]
#         sun = df2.to_dict('records')
#         print(sun)
#         #obj2 =[{'id': x[0], 'table': x[1],'schema':x[2],'database':x[3],'column_name':x[4],'datatype':x[5],'orginal_position':x[6]} for x in sun]
#         #sss=obj,obj2
#         #ss=json.dumps(sss)
# return Response(s)

