#import libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Define the default arguments for the DAG
default_args = {
    'owner': 'humaira',
    'start_date': days_ago(0),
    'email': ['abc123@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)


# Task 1- to unzip the data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)

# Task to extract data from CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f"awk -F',' 'BEGIN {{OFS=\",\"}} NR==1 {{print \"Rowid\",\"Timestamp\",\"Anonymized Vehicle number\",\"Vehicle type\"}} NR>1 {{print $1,$2,$3,$4}}' /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv",
    dag=dag,
)

# Task to extract data from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f"awk -F'\\t' 'BEGIN {{OFS=\",\"}} NR==1 {{print \"Number of axles\",\"Tollplaza id\",\"Tollplaza code\"}} NR>1 {{print $1,$2,$3}}' /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv",
    dag=dag,
)

# Task to extract data from fixed-width file
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f"awk -F'\\t' 'BEGIN {{OFS=\",\"}} NR==1 {{print \"Type of Payment code\",\"Vehicle Code\"}} NR>1 {{print $1,$2}}' /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag=dag,
)

# Task to consolidate data into a single CSV file
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)

# Task to transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f"awk -F',' 'BEGIN {{OFS=\",\"}} NR==1 {{print}} NR>1 {{print $1,$2,$3,toupper($4)}}' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag,
)

# Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data