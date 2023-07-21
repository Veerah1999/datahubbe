from django.http.response import Http404
from datahub_v3_app.models import connection_detail, pipeline_schedule
from rest_framework.views import APIView
from connection_details_api.serializers import Connection_Details_Serializer
from rest_framework.response import Response
from django.db import connections
import mysql.connector as msql
import mysql.connector.errors as DatabaseErrors
from rest_framework.views import APIView
import snowflake.connector as sfc
from snowflake.connector.errors import DatabaseError, ProgrammingError
import teradatasql
from teradata.api import ProgrammingError, DatabaseError
import pyodbc
from pyodbc import ProgrammingError, DatabaseError
import psycopg2 as ps
from psycopg2 import ProgrammingError, DatabaseError
from rest_framework.response import Response
import cx_Oracle



class Connectiton_Detail_View(APIView):
    def get_object(self, pk):
            try:
                return connection_detail.objects.get(pk=pk)
            except connection_detail.DoesNotExist:
                raise Http404
    def get(self, request, pk=None, format=None):
        var_pipeline_det_id= connection_detail.objects.filter(id=3).values('con_str')
        print(var_pipeline_det_id)
        # temp_pd_id=pipeline_det_id[0]
        if pk:
                data = self.get_object(pk)
                var_serializer = Connection_Details_Serializer(data)
                return Response([var_serializer.data])

        else:
                data = connection_detail.objects.all()
                var_serializer = Connection_Details_Serializer(data, many=True)

                return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        var_serializer = Connection_Details_Serializer(data=data)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'connect_detail Created Successfully',
            'data': var_serializer.data
        }
        return response

    def put(self, request, pk=None, format=None):
        var_update_conn_details = connection_detail.objects.get(pk=pk)
        var_serializer = Connection_Details_Serializer(instance=var_update_conn_details,data=request.data, partial=True)
        var_serializer.is_valid(raise_exception=True)
        var_serializer.save()
        response = Response()
        response.data = {
            'message': 'conect_detail Updated Successfully',
            'data': var_serializer.data
        }
        return response

    def delete(self, request, pk, format=None):
        var_delete_conn_details =  connection_detail.objects.get(pk=pk)
        var_delete_conn_details.delete()
        return Response({
            'message': 'connect_detail Deleted Successfully'
        })
    
class test_con_source(APIView):
     def post(self,request,):
        data=request.data
        
        sources=data['con_str']
        source_connection_id=data['connection_id']
        if source_connection_id == 1:
                data_set=snowflake(sources=sources)
        elif source_connection_id == 2:
                data_set=postgres(sources=sources)
        elif source_connection_id == 3:
                data_set=mysql(sources=sources)
        elif source_connection_id == 4:
                data_set=oracle(sources=sources)
        elif source_connection_id == 5:
                data_set=teradata(sources=sources)
        elif source_connection_id == 6:
                data_set=mssql(sources=sources)
        return Response(data_set)

    
def mysql(sources):
            try:
                connection = msql.connect(
                    host=sources['host'],
                    user=sources['user'],
                    password=sources['password'],
                    port=sources['port'],
                    database=sources['database']
                )
                cursor = connection.cursor()

                # Do something with the database connection here...
                return {True: "Connection success"}

            except DatabaseErrors.ProgrammingError as e:
                error_message = str(e)
                return {False: f"Programming error: {error_message}"}

            except DatabaseErrors.InterfaceError as e:
                error_message = str(e)
                return {False: f"Connection error: {error_message}"}

            except DatabaseErrors.DatabaseError as e:
                error_message = str(e)
                return {False: f"Database error: {error_message}"}

            except Exception as e:
                error_message = str(e)
                return {False: f"Unknown error: {error_message}"}

def snowflake(sources):
    try:
        conn = sfc.connect(
            user=sources['user'],
            password=sources['password'],
            account=sources['account'],
            warehouse=sources['warehouse'],
            database=sources['database'],
            schema=sources['schema'],
            role=sources['role']
        )
        cursor = conn.cursor()
        return {True: "Connection success"}

    except ProgrammingError as e:
        error_message = str(e)
        return {False: f"Programming error: {error_message}"}

    except DatabaseError as e:
        error_message = str(e)
        return {False: f"Database error: {error_message}"}

    except Exception as e:
        error_message = str(e)
        return {False: f"Unknown error: {error_message}"}


def teradata(sources):
    try:
        conn = teradatasql.connect(
            host=sources['server'],
            user=sources['username'],
            password=sources['password'],
            encryptdata='true',
        )
        cursor = conn.cursor()

        return {True: "Connection success"}

    except ProgrammingError as e:
        error_message = str(e)
        return {False: f"Programming error: {error_message}"}

    except DatabaseError as e:
        error_message = str(e)
        return {False: f"Database error: {error_message}"}

    except Exception as e:
        error_message = str(e)
        return {False: f"Unknown error: {error_message}"}


def mssql(sources):
    try:
        conn = pyodbc.connect(
            DRIVER=sources['driver'],
            SERVER=sources['server'],
            PORT=sources['port'],
            DATABASE=sources['database'],
            UID=sources['username'],
            PWD=sources['password']
        )
        cursor = conn.cursor()

        return {True: "Connection success"}

    except ProgrammingError as e:
        error_message = str(e)
        return {False: f"Programming error: {error_message}"}

    except DatabaseError as e:
        error_message = str(e)
        return {False: f"Database error: {error_message}"}

    except Exception as e:
        error_message = str(e)
        return {False: f"Unknown error: {error_message}"}


def postgres(sources):
    try:
        conn = ps.connect(
            database=sources['database'],
            user=sources['user'],
            password=sources['password'],
            host=sources['host'],
            port=sources['port']
        )
        cursor = conn.cursor()

        return {True: "Connection success"}

    except ProgrammingError as e:
        error_message = str(e)
        return {False: f"Programming error: {error_message}"}

    except DatabaseError as e:
        error_message = str(e)
        return {False: f"Database error: {error_message}"}

    except Exception as e:
        error_message = str(e)
        return {False: f"Unknown error: {error_message}"}


def oracle(sources):
    try:
        connstr = f"{sources['user']}/{sources['password']}@{sources['host']}:{sources['port']}/{sources['database']}"

        conn = cx_Oracle.connect(connstr)

        return {True: "Connection success"}

    except ProgrammingError as e:
        error_message = str(e)
        return {False: f"Programming error: {error_message}"}

    except DatabaseError as e:
        error_message = str(e)
        return {False: f"Database error: {error_message}"}

    except Exception as e:
        error_message = str(e)
        return {False: f"Unknown error: {error_message}"}
