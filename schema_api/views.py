
from django.http.response import Http404
from datahub_v3_app.models import schema_migration
from rest_framework.views import APIView
from schema_api.serializers import Schema_Serializer
from rest_framework.response import Response
from schema_framework.views import *


class Schema_View(APIView):
    def get_object(self, pk):
        try:
            return schema_migration.objects.get(pk=pk)
        except schema_migration.DoesNotExist:
            raise Http404

    def get(self, request, pk=None, format=None):
        if pk:
            data = self.get_object(pk)
            var_serializer = Schema_Serializer(data)
            return Response(var_serializer.data)
        else:
            data = schema_migration.objects.all()
            var_serializer = Schema_Serializer(data, many=True)

            return Response(var_serializer.data)

    def post(self, request, format=None):
        data = request.data
        serializer = Schema_Serializer(data=data)

        serializer.is_valid(raise_exception=True)

        pass_set = serializer.save()
        set = {"id": pass_set.id}
        response = Response()

        response.data = {
            'message': 'connect_detail Created Successfully',
            'data': set
        }
        return response

    def put(self, request, pk=None, format=None):
        var_update_scheme = schema_migration.objects.get(pk=pk)
        var_serializer = Schema_Serializer(instance=var_update_scheme, data=request.data, partial=True)

        var_serializer.is_valid(raise_exception=True)

        var_serializer.save()

        response = Response()

        response.data = {
            'message': 'Schema Updated Successfully',
            'data': var_serializer.data
        }

        return response

    def delete(self, request, pk, format=None):
        var_delete_schema = schema_migration.objects.get(pk=pk)

        var_delete_schema.delete()

        return Response({
            'message': 'Schema Deleted Successfully'
        })


class soft(APIView):

    def put(self, request, pk):
       # import pdb
       # pdb.set_trace()
        print(pk)
        schema_fram(schema_id=pk)
        return Response("Done")

class table_schema_get(APIView):
     def get(request,self,pk):
        sources_connection=connection_detail.objects.filter(id=pk).values()
        sources_temp={}
        for i in sources_connection:
                sources_temp.update(i)
        source_connection_id = sources_temp['connection_id_id']
        sources=sources_temp['con_str']

        if source_connection_id == 3:
            data_set = mysql_4(sources=sources)
        elif source_connection_id == 1:
            data_set = snow_1(sources=sources)
        elif source_connection_id == 2:
            data_set = postgres_6(source=sources)
        elif source_connection_id == 6:
            data_set = sql_server_2(sources=sources)
        elif source_connection_id == 4:
            data_set = oracle_3(sources=sources)
        elif source_connection_id == 5:
            data_set = teradata_5(sources=sources)
  
        return  Response(data_set)
class table_schema_get1(APIView):
     def get(self,request,pk):
       # import pdb
       # pdb.set_trace()
        sources_connection=connection_detail.objects.filter(id=pk).values()
        sources_temp={}
        for i in sources_connection:
                sources_temp.update(i)
        source_connection_id = sources_temp['connection_id_id']
        sources=sources_temp['con_str']

        if source_connection_id == 3:
            data_set = mysql_4(sources=sources)
        elif source_connection_id == 1:
            data_set = snow_1(sources=sources)
        elif source_connection_id == 2:
            data_set = postgres_6(source=sources)
        elif source_connection_id == 6:
            data_set = sql_server_2(sources=sources)
        elif source_connection_id == 4:
            data_set = oracle_3(sources=sources)
        elif source_connection_id == 5:
            data_set = teradata_5(sources=sources)
     
        return  Response(data_set)

def snow_1(sources):
            url = snowflake.connector.connect(
                user=sources['user'],
                password=sources['password'],
                account=sources['account'],
                warehouse=sources['warehouse'],
                database=sources['database'],
                schema=sources['schema'],
                role=sources['role']
            )

            cur = url.cursor()
            
            # Select the target database
            script2 = f"use {sources['database']}"
            cur.execute(script2)
            script3 = f"use WAREHOUSE {sources['warehouse']}"
            cur.execute(script3)

            # Select all schema names in the database
            script = "SELECT schema_name FROM information_schema.schemata"
            cur.execute(script)
            schema_names = [row[0] for row in cur.fetchall()]

            # Retrieve table names for each schema
            data = []
            for i, schema_name in enumerate(schema_names):
                scriptd = f"""SELECT TABLE_NAME
              FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_SCHEMA = '{schema_name}'"""
                cur.execute(scriptd)
                table_names = [row[0] for row in cur.fetchall()]
                schema_data = {
                    'id': i + 1,
                    'source_id': 1,
                    'schema_names': schema_name,
                    'table_names': table_names
                }
                data.append(schema_data)

            return (data)

def sql_server_2(sources):

            url = pyodbc.connect(
                DRIVER=sources['driver'],
                SERVER=sources['server'],
                PORT=sources['port'],
                DATABASE=sources['database'],
                UID=sources['username'],
                PWD=sources['password']
            )
            cur = url.cursor()

            # Select the target database
            script2 = f"use {sources['database']}"
            cur.execute(script2)

            # Select all schema names in the database
            script = "SELECT schema_name FROM information_schema.schemata"
            cur.execute(script)
            schema_names = [row[0] for row in cur.fetchall()]

            # Retrieve table names for each schema
            data = []
            for i, schema_name in enumerate(schema_names):
                scriptd = f"""SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{schema_name}'"""
                cur.execute(scriptd)
                table_names = [row[0] for row in cur.fetchall()]
                schema_data = {
                    'id': i + 1,
                    'source_id': 1,
                    'schema_names': schema_name,
                    'table_names': table_names
                }
                data.append(schema_data)

            return (data)

def oracle_3(sources):

            connstr = f"{sources['user']}/{sources['password']}@{sources['host']}:{sources['port']}/{sources['database']}"

            conn = cx_Oracle.connect(connstr)
            # cur = conn.cursor()
            cursor = conn.cursor()

            # Retrieve all schema names in the database
            cursor.execute("SELECT USERNAME FROM ALL_USERS WHERE DEFAULT_TABLESPACE NOT IN ('SYSTEM','SYSAUX')")
            schema_names = [row[0] for row in cursor.fetchall()]

            # Retrieve table names for each schema
            data = []
            for i, schema_name in enumerate(schema_names):
                cursor.execute(f"SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '{schema_name}'")
                table_names = [row[0] for row in cursor.fetchall()]
                schema_data = {
                    'id': i + 1,
                    'source_id': 1,
                    'schema_names': schema_name,
                    'table_names': table_names
                }
                data.append(schema_data)

            cursor.close()
            conn.close()

            return (data)


def mysql_4(sources):

            conn = msql.connect(host=sources['host'],
                                 user=sources['user'],
                                 password=sources['password'],
                                 port=sources['port'],
                                 database=sources['database'])
            cursor = conn.cursor()

            cursor.execute("SHOW DATABASES")
            schema_names = [row[0] for row in cursor.fetchall()]

            # Retrieve table names for each schema
            data = []
            for i, schema_name in enumerate(schema_names):
                cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema_name}'")
                table_names = [row[0] for row in cursor.fetchall()]
                schema_data = {
                    'id': i + 1,
                    'source_id': 1,
                    'schema_names': schema_name,
                    'table_names': table_names
                }
                data.append(schema_data)

            cursor.close()
            conn.close()

            return (data)


def teradata_5(sources):
                # udaExec = teradata.UdaExec(appName="MyApp", version="1.0", logConsole=False)
                # session = udaExec.connect(method="odbc", system='<your_system_name>', username='<your_username>',
                #                           password='<your_password>', driver='Teradata')

            url = teradatasql.connect(
                host=sources['server'],
                user=sources['username'],
                password=sources['password'],
                encryptdata='true',
            )
            cur = url.cursor()

            # Retrieve all schema names in the database
            schema_query = "SELECT databasename FROM dbc.databases WHERE ownername = 'DBC'"
            schema_names = [row[0] for row in cur.execute(schema_query)]

            # Retrieve table names for each schema
            data = []
            for i, schema_name in enumerate(schema_names):
                table_query = f"SELECT tablename FROM dbc.tables WHERE databasename = '{schema_name}' AND tablekind = 'T'"
                table_names = [row[0] for row in cur.execute(table_query)]
                schema_data = {
                    'id': i + 1,
                    'source_id': 1,
                    'schema_names': schema_name,
                    'table_names': table_names
                }
                data.append(schema_data)

            cur.close()

            return (data)

def postgres_6(sources):

            conn = ps.connect(
                database=sources['database'],
                user=sources['user'],
                password=sources['password'],
                host=sources['host'],
                port=sources['port']
            )
            cursor = conn.cursor()

            # Retrieve all schema names in the database
            cursor.execute("SELECT schema_name FROM information_schema.schemata")
            schema_names = [row[0] for row in cursor.fetchall()]

            # Retrieve table names for each schema
            data = []
            for i, schema_name in enumerate(schema_names):
                cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'")
                table_names = [row[0] for row in cursor.fetchall()]
                schema_data = {
                    'id': i + 1,
                    'source_id': 1,
                    'schema_names': schema_name,
                    'table_names': table_names
                }
                data.append(schema_data)

            cursor.close()
            conn.close()

            return (data)
