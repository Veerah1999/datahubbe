from google.cloud import storage
import psycopg2
import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
import snowflake.connector
import time
start = time.time()

# Connect to PostgreSQL database
conn = psycopg2.connect(database='vishwa',
                        user='vishwa',
                        password='Vishwa@6',
                        host='34.125.249.183',
                        port='5432'
                        )

# conn = psycopg2.connect(database='migration_test',
#                         user='postgres',
#                         password='dhub1112',
#                         host='34.23.32.248',
#                         port='5432'
#                         )
cur = conn.cursor()
print('source connected..')
# Extract data using COPY command
# with open('data.csv', 'w') as f:
#     cur.copy_expert("COPY demo TO STDOUT DELIMITER ',' CSV HEADER", f)

# with open('dummy3.csv', 'w') as f:
#     cur.copy_expert("COPY demo2 TO STDOUT  CSV HEADER", f)


# df = pd.read_sql_query("SELECT * FROM demo2 ", conn)
# df.to_csv('dummy5.csv', index=False)


df = pd.read_sql("SELECT * FROM demo", conn)
df.to_parquet('gcp_url', index=False)

# gcp_url = 'gcs://databucket8806/'
gcp_url = 'gcs://datahub-bkt/'

# path_to_private_key = 'C:/vishwa/pythonProject/parallel load/static-anchor-375606-7a7ec4595426.json'
path_to_private_key = 'E:\Datahub_V3\datahub-v3\datahub_v3_project\dm-solutions-474f4a15dff8.json'
client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)

# bucket = client.bucket('databucket8806')
bucket = client.bucket('datahub-bkt')
blob = bucket.blob('vishwa_test4.parquet')
blob.upload_from_filename('gcp_url')
print('file loaded to gcs..')


# # target
url = snowflake.connector.connect(
    source2="snowflake",
    user="vishwas",
    password="Vishwa@6",
    account="jqouozn-jq36688",
    warehouse="COMPUTE_WH",
    database="TALEND",
    schema="PUBLIC",
    role="ACCOUNTADMIN"
)

cur= url.cursor()
print(" connected to snowflake...")

# create storage integration
cur.execute("""CREATE or replace STORAGE INTEGRATION gcs_inter
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://datahub-bkt/');""")

# print("storage integration created...")

# stage creation
cur.execute("""create or replace stage my_gcs_stage
 url = 'gcs://datahub-bkt/vishwa_test4.parquet'
 storage_integration = gcs_inter
 FILE_FORMAT = (format_name = My_parquet)""")

# print('stage created successfully..')

cur.execute("""create or replace file format My_parquet
 type = 'parquet'""")

# copy into target table
cur.execute("""
   copy into test6 from @my_gcs_stage
   FILE_FORMAT=(format_name = My_parquet)
   MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
   on_error = 'skip_file';""")

cur.execute('drop stage my_gcs_stage ')

print('data loaded to table..')
# cur.execute("drop stage my_gcs_stage")
cur.close()
# print('stage dropped..')