from prefect import flow, task

import fitbit
from fitbit import gather_keys_oauth2 as Oauth2
import pandas as pd
import datetime
import json

with open("fitbit_login_info.json", "r") as openfile:
    fitbit_info=json.load(openfile)

CLIENT_ID = fitbit_info['CLIENT_ID']
CLIENT_SECRET = fitbit_info['CLIENT_SECRET']

server = Oauth2.OAuth2Server(CLIENT_ID, CLIENT_SECRET)
server.browser_authorize()
ACCESS_TOKEN = str(server.fitbit.client.session.token['access_token'])
REFRESH_TOKEN = str(server.fitbit.client.session.token['refresh_token'])
auth2_client = fitbit.Fitbit(CLIENT_ID, CLIENT_SECRET, oauth2=True, access_token=ACCESS_TOKEN, refresh_token=REFRESH_TOKEN)


@task
def gcs_pull_task():
    print('\n\n GCS pull task started! \n\n')

    from google.cloud import storage
    import json
    import os
    import sys

    PATH=os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    storage_client = storage.Client(PATH)
    bucket=storage_client.get_bucket('tf_test_lake_neon-runway-359404')

    # pull data
    blop = bucket.blob(blob_name = 'master_table.csv').download_as_string()

    # write data to a file
    with open ('master_table.csv', "wb") as f:
        f.write(blop)
    
    print('\n\n GCS pull task finished! \n\n')

@task
def fitbit_task():
    print('\n\n fitbit task started! \n\n')
    today = str(datetime.datetime.now().strftime("%Y-%m-%d"))

    fit_statsHR = auth2_client \
        .intraday_time_series('activities/heart',
            base_date=today, detail_level='1sec')

    time_list = []
    val_list = []
    for i in fit_statsHR['activities-heart-intraday']['dataset']:
        val_list.append(i['value'])
        time_list.append(i['time'])

    dto=datetime.datetime.now()-datetime.timedelta(minutes=3)
    time_3_min_ago=dto.strftime("%H:%M:%S")

    heartdf = pd.DataFrame({'Heart Rate':val_list,'Time':time_list})
    heartdf= heartdf.loc[(heartdf['Time']>=time_3_min_ago), ['Heart Rate','Time']]

    print('\n\n fitbit task done! \n\n')
    return heartdf

@task
def pyspark_task(heartdf):
    print('\n\n spark task started! \n\n')

    #import/start spark session
    import pyspark
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.master('local[*]').appName('test2').getOrCreate()

    #read fitbit pandas DF into a spark DF
    recent_rates_df=spark.createDataFrame(heartdf)
 
    #register the spark df as a temp table, query it for the recent max
    recent_rates_df.registerTempTable('recent_rates_df')
    temp_max_df=spark \
        .sql("select * from recent_rates_df order by 1 desc limit 1")

    #load/format master_table
    master_table = spark.read.option("header",True).csv("master_table.csv")

    #union master table with the recent max
    master_table = master_table.union(temp_max_df)

    #overwrite the master table
    pd_master_table=master_table.toPandas()

    spark.stop()
    print('\n\n spark task done! \n\n')
    
    return pd_master_table

@task
def overwrite_master_table(pd_master_table):
    
    print('\n\n overwrite task started! \n\n')

    import os
    pd_master_table.to_csv('master_table.csv', index=False)

    print('\n\n overwrite task done! \n\n')

@task
def gcs_push_task():
    print('\n\n GCS push task started! \n\n')
    from google.cloud import storage
    import json
    import os
    import sys

    PATH=os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    storage_client = storage.Client(PATH)
    bucket=storage_client.get_bucket('tf_test_lake_neon-runway-359404')

    filename='master_table.csv'
    UPLOADFILE = os.path.join(os.getcwd(),filename)
    bucket = storage_client.get_bucket('tf_test_lake_neon-runway-359404')
    blob=bucket.blob(filename)
    blob.upload_from_filename(UPLOADFILE)

    print('\n\n GCS push task done! \n\n')


@flow
def pushpullflow():
    print("\n\n flow started : O \n\n")

    gcs_pull_task()
    overwrite_master_table(pyspark_task(fitbit_task()))
    gcs_push_task()
    print("\n\n flow done :) \n\n")

if __name__ == "__main__":
    pushpullflow()
