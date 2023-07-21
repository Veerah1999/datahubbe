from django.shortcuts import render
from django.http import HttpResponse, FileResponse
from openpyxl.workbook import Workbook
import openpyxl
import pandas as pd
import psycopg2 as ps
from rest_framework.views import APIView
from rest_framework.response import Response
import json
import snowflake.connector as sf
import pyodbc
import numpy as np


class Audit(APIView):


    def post(self, request, format=None):
        name = request.data["user"]
        userpwd = request.data["password"]
        database = request.data["database"]
        host = request.data["host"]
        port = request.data["port"]
        #account = request.data["account"]
        #warehouse = request.data["warehouse"]
        #schema = request.data["schema"]
        #role = request.data["role"]

        def getColumnDtypes(dataTypes):

            dataList = []
            for x in dataTypes:
                if (x == 'bigint'):
                    dataList.append('int')
                elif (x == 'integer'):
                    dataList.append('int')
                elif (x == 'text'):
                    dataList.append('varchar')
                elif (x == 'date'):
                    dataList.append('DATE')
                elif (x == 'boolean'):
                    dataList.append('bool')
                elif (x == 'smallint'):
                    dataList.append('int')
                else:
                    dataList.append('varchar')

            return dataList
        url = ps.connect(
            user=name,
            password=userpwd,
            database=database,
            host=host,
            port=port
        )
        cursor = url.cursor()
        print("connected")
        script = """
    with tbl as (SELECT table_schema,table_name,table_catalog, pg_relation_size(quote_ident(table_name)) FROM information_schema.tables   where table_name not like 'pg_%' and table_schema in ('public')) 
 select table_schema, table_name,table_catalog,pg_relation_size(quote_ident(table_name)), (xpath('/row/c/text()',
 query_to_xml(format('select count(*) as c from %I.%I', table_schema, table_name), false, true, '')))[1]::text::int as rows_n from tbl ORDER BY 3 DESC;

    """

        cursor.execute(script)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(list(data), columns=columns)
        df['SOURCE'] = 'postgres'
        df['TARGET'] = 'snowflake'

        # writer = pd.ExcelWriter(r"C:\Users\Public\Downloads\audit_report.xlsx")
        script2= '''select a.table_schema ,a.table_name ,a.table_catalog ,a.data_type,a.column_name,a.ordinal_position from information_schema.columns a left join information_schema.tables b on
         ( a.table_name = b.table_name) where a.table_schema in('public');'''
        cursor.execute(script2)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df2 = pd.DataFrame(list(data), columns=columns)
        dummy1 = getColumnDtypes(df2['data_type'].tolist())
        df2['TARGET_DATA_TYPE'] = dummy1
        df2['SOURCE'] = 'postgres'
        df2['TARGET'] = 'snowflake'
        s= df.to_records()
        print(s)
        obj = [{'id': x[0], 'schema': x[1],'table':x[2],'database':x[3],'table_size':x[4],'row_count':x[5],'source':x[6],'target':x[7]} for x in s]
        sun=df2.to_records()
        print(sun)
        obj2 =[{'id': x[0], 'schema': x[1],'table':x[2],'database':x[3],'datatype':x[4],'column_name':x[5],'orginal_position':x[6],'target_data_type':x[7],'source':x[8],'target':x[9]} for x in sun]
        sss=obj,obj2


        return Response(sss)
        # df.to_excel(writer, sheet_name='table')
        # df2.to_excel(writer, sheet_name='datatype')
        # writer.save()
        # df3=df.to_dict(),df2.to_dict()
        # return Response(df3)
        
        # return Response("Downloaded in your User/Public/Downloads")

class AuditSnow(APIView):

    def post(self, request, format=None):
        name = request.data["user"]
        userpwd = request.data["password"]
        database = request.data["database"]
        # host = request.data["host"]
        # port = request.data["port"]
        account = request.data["account"]
        warehouse = request.data["warehouse"]
        schema = request.data["schema"]
        role = "ACCOUNTADMIN"

        def getColumnDtypes(dataTypes):

            dataList = []
            for x in dataTypes:
                if (x == 'bigint'):
                    dataList.append('int')
                elif (x == 'integer'):
                    dataList.append('int')
                elif (x == 'text'):
                    dataList.append('varchar')
                elif (x == 'date'):
                    dataList.append('DATE')
                elif (x == 'boolean'):
                    dataList.append('bool')
                elif (x == 'smallint'):
                    dataList.append('int')
                else:
                    dataList.append('varchar')

            return dataList

        url = sf.connect(
            user=name,
            password=userpwd,
            database=database,
            account=account,
            schema=schema,
            warehouse=warehouse,
            role=role,
        )
        cursor = url.cursor()
        sql=f"use {database}"
        cursor.execute(sql)
        print("connected")
        script = """
   select a.TABLE_NAME , a.TABLE_SCHEMA, a.TABLE_CATALOG,jim.BYTES,jim.ROW_COUNT from snowflake.account_usage.tables a 
    left join INFORMATION_SCHEMA.tables jim on (a.TABLE_NAME = jim.TABLE_NAME) ;
    """
        cursor.execute(script)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(list(data), columns=columns)
        df['SOURCE'] = 'snowflake'
        df['TARGET'] = 'oracle'
        # sql1=f"use{database}"
        # cursor.execute(sql1)
        script2= '''     
  select  a.TABLE_NAME , a.TABLE_SCHEMA, a.TABLE_CATALOG,b.COLUMN_NAME ,b.DATA_TYPE,b.ORDINAL_POSITION from 
  snowflake.account_usage.tables a left join INFORMATION_SCHEMA.columns b on (b.TABLE_NAME = a.TABLE_NAME) ;'''
        cursor.execute(script2)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df2 = pd.DataFrame(list(data), columns=columns,index=None)
        dummy1 = getColumnDtypes(df2['DATA_TYPE'].tolist())
        df2['TARGET_DATA_TYPE'] = dummy1
        df2['SOURCE'] = 'postgres'
        df2['TARGET'] = 'snowflake'
        s= df.to_records()
        print(s)
        obj = [{'id': x[0], 'table': x[1],'schema':x[2],'database':x[3],'table_size':x[4],'row_count':x[5],'source':x[6],'target':x[7]} for x in s]
        sun=df2.to_records()
        print(sun)
        obj2 =[{'id': x[0], 'table': x[1],'schema':x[2],'database':x[3],'column_name':x[4],'datatype':x[5],'orginal_position':x[6],'target_data_type':x[7],'source':x[8],'target':x[9]} for x in sun]
        sss=obj,obj2


        return Response(sss)

class AuditSql(APIView):

    def post(self, request, format=None):
        name = request.data["user"]
        userpwd = request.data["password"]
        database = request.data["database"]
        host = request.data["host"]
        port = request.data["port"]
        Driver = "ODBC Driver 17 for SQL Server"

        def getColumnDtypes(dataTypes):

            dataList = []
            for x in dataTypes:
                if (x == 'bigint'):
                    dataList.append('int')
                elif (x == 'integer'):
                    dataList.append('int')
                elif (x == 'text'):
                    dataList.append('varchar')
                elif (x == 'date'):
                    dataList.append('DATE')
                elif (x == 'boolean'):
                    dataList.append('bool')
                elif (x == 'smallint'):
                    dataList.append('int')
                else:
                    dataList.append('varchar')

            return dataList

        url = pyodbc.connect(
            DRIVER=Driver,
            SERVER=host,
            PORT=port,
            UID=name,
            PWD=userpwd,
            DATABASE=database,

        )
        cursor = url.cursor()
        # import pdb
        # pdb.set_trace()
        print("connected")

        sql = f"use {database}"
        cursor.execute(sql)
        script = """
 SELECT 
    t.name AS TableName, 
    SCHEMA_NAME(t.schema_id) AS SchemaName, 
    SUM(p.row_count) AS RowCounts, 
    SUM(p.reserved_page_count) * 8 AS TotalSizeInKB
FROM 
    sys.tables t
INNER JOIN 
    sys.dm_db_partition_stats p ON t.OBJECT_ID = p.OBJECT_ID
WHERE 
    t.is_ms_shipped = 0 
GROUP BY 
    t.Name, SCHEMA_NAME(t.schema_id)
ORDER BY 
    TotalSizeInKB DESC;
    """
        cursor.execute(script)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(np.array(data),columns=columns)
        df['SOURCE'] = 'Sql Server'
        df['TARGET'] = 'Snowflake'
        # sql1 = f"use{database}"
        # cursor.execute(sql1)
        script2 = '''SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE FROM information_schema.COLUMNS;'''
        cursor.execute(script2)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df2 = pd.DataFrame(np.array(data), columns=columns, index=None)
        dummy1 = getColumnDtypes(df2['DATA_TYPE'].tolist())
        df2['TARGET_DATA_TYPE'] = dummy1
        df2['SOURCE'] = 'Sql Server'
        df2['TARGET'] = 'Snowflake'
        s = df.to_records()
        print(s)
        obj = [{'id': x[0], 'table': x[1], 'schema': x[2], 'row_count': x[3], 'table_size': x[4], 'source': x[5],
                'target': x[6]} for x in s]
        sun = df2.to_records()
        print(sun)
        obj2 = [
            {'id': x[0], 'schema': x[1], 'table': x[2], 'column_name': x[3], 'orginal_position': x[4], 'datatype': x[5],
             'target_data_type': x[6], 'source': x[7], 'target': x[8]} for x in sun]
        sss = obj, obj2

        return Response(sss)
