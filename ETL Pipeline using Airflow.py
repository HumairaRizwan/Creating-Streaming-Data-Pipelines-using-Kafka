from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'humaira',
    'start_date': days_ago(0),
    'email': ['hello90@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
} 

# defining the DAG
dag = DAG(
    dag_id = 'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Project',
    schedule_interval=timedelta(days=1),
)

# define the tasks
unzip_data = BashOperator(
    task_id= 'unzip_data',
    bash_command= 'tar -xzvf /home/project/airflow/dags/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag= dag,
)

# define the tasks to update
extract_data_from_csv = BashOperator(
    task_id= 'extract_data_from_csv',
    bash_command= 'cut -d"," -f1-4 < /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag= dag,
)

# define the tasks 1.5
extract_data_from_tsv = BashOperator(
    task_id= 'extract_data_from_tsv',
    bash_command= 'cut -f5-7 < /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag= dag,
)

# define the tasks 1.6
extract_data_from_fixed_file = BashOperator(
    task_id= 'extract_data_from_fixed_file',
    bash_command= 'cut -c 59-68 < /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag= dag,
)


# define the tasks 1.7
consolidate_data = BashOperator(
    task_id= 'consolidate_data',
    bash_command= 'paste /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag= dag,
)


# define the tasks 1.8
Transform_data = BashOperator(
    task_id= 'Transform_data',
    #bash_command= f"awk 'BEGIN{FS=OFS=","} {split($4, a, "\t"); a[1]=toupper(a[1]); $4=a[1]; for(i=2; i<=length(a); i++) $4=$4"\t"a[i]; print $0}' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed-data.csv",
    bash_command = (
    "awk 'BEGIN{FS=OFS=\",\"} {split($4, a, \"\\t\"); a[1]=toupper(a[1]); $4=a[1]; "
    "for(i=2; i<=length(a); i++) $4=$4\"\\t\"a[i]; print $0}' "
    "/home/project/airflow/dags/finalassignment/extracted_data.csv > "
    "/home/project/airflow/dags/finalassignment/staging/transformed-data.csv"
),
    dag= dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_file >> consolidate_data >> Transform_data
