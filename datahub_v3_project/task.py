import pandas as pd
import snowflake.connector
import snowflake.connector as sff
from celery import shared_task, Task
from celery.utils.log import get_task_logger
from celery.contrib import rdb
import snowflake.connector as sf
import pandas as pd
#import configparser
import pandas
from snowflake.connector.pandas_tools import write_pandas
import psycopg2 as p2
import pandas as pd

logger = get_task_logger(__name__)


# @shared_task
# def sample_task():
#     logger.info("The sample task just ran.")
@shared_task(
    base=Task, queue="scheduler", bind=True
)

def sample_function(self, arg1,arg2):
    """write logic here"""
    print('celery is working fine ganesh')
    print(arg2)
    dataset=arg2
    source = dataset[0]
    target = dataset[1]
    print(source)
    print(target)

    source_table_name = source['source_table_name']

    target_table_name = target['target_table_name']
    st=source_sf(source,source_table_name)
    tt=target_sf(target,source_table_name,target_table_name)



def source_sf(source,source_table_name):
    url = snowflake.connector.connect(
    user=source['user'],
    password=source['password'],
    account=source['account'],
    warehouse=source['warehouse'],
    database=source['database'],
    schema=source['schema'],
    role='ACCOUNTADMIN'
    )

    cur = url.cursor()
    print("sf connected")
    sql = f"select * from {source_table_name}"
    cur.execute(sql)

    df = pd.DataFrame(cur)
    print('read from emp table', df)

# establishing the connection
def target_sf(target,source_table_name,target_table_name):
    conn = p2.connect(
        database=target['database'],
        user=target['user'],
        password=target['password'],
        host=target['host'],
        port= target['port'],
    )
    cur = conn.cursor()

    sql =  f"insert into  {target_table_name} (select * from {source_table_name})"
    cur.execute(sql)

    cur.execute(sql)
    print('data write in postgres')
    conn.commit()
    conn.close()


# def target_sf(target,source_table_name,target_table_name):

#     url = snowflake.connector.connect(
#     user=target['user'],
#     password=target['password'],
#     account=target['account'],
#     warehouse=target['warehouse'],
#     database=target['database'],
#     schema=target['schema'],
#     role='ACCOUNTADMIN'
#     )

#     cur = url.cursor()
#     print("sf connected")

#     sql2 = f"insert into  {target_table_name} (select * from {source_table_name})"
#     # sql2="select * from "
#     print(sql2)
#     cur.execute(sql2)
#     dff = pd.DataFrame(cur)
#     # df.head()
#     print('read from emp table', dff)
#     print('inserted from source to target')