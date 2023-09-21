# This Python script extracts loan application data from offline CSV file to S3 -> SF DB
# Author: Subhajit Maji
# subhajitsr@gmail.com
# +65-98302027
# Date: 2023-09-21

from datetime import timezone, datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import os
import time
import pandas as pd
import boto3
import snowflake.connector
import ast

dag = DAG('loan-application-core-loader-v1',
          description='extracts loan application data from offline CSV file to S3 -> SF DB',
          schedule_interval='0 * * * *',
          start_date=datetime(2023, 9, 21),
          catchup=False)


# AWS Creds
aws_access_key_id = Variable.get("AWS_ACCESS_KEY")
aws_secret_access_key = Variable.get("AWS_SECRET_KEY")
s3_bucket_name = "TEST001"
s3_path = "loan-data/dump"

# Snowflake details
sf_username = Variable.get("SF_USERNAME")
sf_password = Variable.get("SF_PASSWORD")
sf_account = Variable.get("SF_ACCOUNT")
sf_warehouse = 'TEST_WAREHOUSE'
sf_database = 'TESTDB'
sf_schema = 'CORE'
sf_table = 'tbl_loan_application'
s3_stage_name = "stg_s3_loan_application"

# local path of the csv files
local_directory = '/inbox/loan-data'
local_archived_directory = '/archive/loan-data'

# Initialising S3 client
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Initialising Snowflake connection
dbcon = snowflake.connector.connect(
    user=sf_username,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database
)


def fn_load_to_s3(**context):
    # fetch the list of csv files in the path
    csv_files = [f for f in os.listdir(local_directory) if f.endswith('.csv')]

    if csv_files:
        s3_obj_key_list = []
        for file_name in csv_files:
            upload_flag = 0

            # Appending filename with timestamp to avoid file name conflict in S3
            file_name_s3 = f"{file_name}_{str(int(round(time.time())))}"
            # s3 object key
            s3_object_key = f'{s3_path}/{file_name_s3}'

            # Read the CSV to a DataFrame
            df = pd.read_csv(os.path.join(local_directory, file_name))

            # Upload the DataFrame as a CSV file to S3
            try:
                s3_client.put_object(
                    Bucket=s3_bucket_name,
                    Key=s3_object_key,
                    Body=df.to_csv(index=False),
                )
                upload_flag = 1
            except Exception as e:
                raise Exception(f"S3 upload error. {e}")

            if upload_flag == 1:
                logging.info(f'Uploaded {file_name_s3} to S3')
                s3_obj_key_list.append(file_name_s3)
                # Archive the local file
                os.rename(f"{local_directory}/{file_name}", f"{local_archived_directory}/{file_name}")

        context['ti'].xcom_push(key='s3_obj_key_list', value=s3_obj_key_list)
    else:
        logging.info("No new files..")

    logging.info("fn_load_to_s3 completed")


def fn_load_s3_to_sf_wrk(**context):
    s3_obj_list = context['ti'].xcom_pull(key='s3_obj_key_list')
    # Convert the list-String to list (Xcom always stores context variables as string)
    s3_obj_list = ast.literal_eval(s3_obj_list)
    logging.info(f"File list to load: {s3_obj_list}")
    for s3_obj in s3_obj_list:
        logging.info(f"Attempting to load file {s3_obj}")
        try:
            cursor = dbcon.cursor()
            cursor.execute(f"""
                COPY INTO {sf_schema}.{sf_table}(serious_dlq_in_2yrs,revolving_util_of_unsecured_lines,age,
                num_of_time_30_59_days_past_due_not_worse,debt_ratio,monthly_income,num_of_open_cred_ln_n_loans,
                num_of_times_90days_late,num_real_estate_loans_or_lines,num_of_time_60_89_day_past_due_nt_worse,
                number_of_dependents,_file_name,_load_ts)
                FROM (select hdr.$2, hdr.$3, hdr.$4, hdr.$4, hdr.$5,hdr.$6, hdr.$7, hdr.$8, hdr.$9, hdr.$10, hdr.$11, 
                '{s3_obj}','{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' from @%{s3_stage_name}/{s3_obj} hdr)
                FILE_FORMAT = (TYPE = 'CSV')"""
                )
            logging.info(f"Successfully loaded data from {s3_obj} "
                         f"into Snowflake table {sf_schema}.{sf_table}.")
        except Exception as e:
            raise Exception(f"{s3_obj} File to DB load error. {e}")
        finally:
            cursor.close()

    logging.info("fn_load_s3_to_sf completed")
    dbcon.close()


# Define tasks
task_load_to_s3 = PythonOperator(
    task_id='task_load_to_s3',
    python_callable=fn_load_to_s3,
    provide_context=True,
    dag=dag,
)

task_load_s3_to_sf = PythonOperator(
    task_id='task_load_s3_to_sf',
    python_callable=fn_load_s3_to_sf_wrk,
    provide_context=True,
    dag=dag,
)

# Define task dependency
task_load_to_s3 >> task_load_s3_to_sf


