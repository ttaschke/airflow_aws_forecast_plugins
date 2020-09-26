from datetime import datetime
import logging
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from operators.aws_forecast_import_operator import AwsForecastImportOperator
from operators.aws_forecast_operator import AwsForecastOperator
from operators.aws_forecast_export_operator import AwsForecastExportOperator
from sensors.aws_forecast_import_sensor import AwsForecastImportSensor
from sensors.aws_forecast_sensor import AwsForecastSensor
from sensors.aws_forecast_export_sensor import AwsForecastExportSensor


dag_schedule = None

dag = DAG(
    dag_id='forecast_example',
    description='Example DAG for AWS Forecast-related plugins',
    schedule_interval=dag_schedule,
    start_date=datetime(2018, 10, 11),
    max_active_runs=1,
)

create_import_job_task = AwsForecastImportOperator(
    task_id='create_import_job',
    config={
        'DatasetImportJobName': 'electricity_ds_import_job_{{ ds_nodash }}',
        'DatasetArn': 'arn:aws:forecast:us-west-2:acct-id:dataset/electricity_demand_ds',
        'DataSource': {
            'S3Config': {
                'Path': 's3://your-bucket/training_data/electricityusagedata.csv',
                'RoleArn': 'arn:aws:iam::acct-id:role/forecast_role'
            }
        },
    },
    aws_conn_id='aws_default',
    dag=dag
)

wait_for_import_job_task = AwsForecastImportSensor(
    task_id='wait_for_import_job',
    import_job_arn="{{ task_instance.xcom_pull(task_ids='create_import_job')}}",
    aws_conn_id='aws_default',
    timeout=20 * 60,  # timeout(seconds) for Sensor polling (20 min)
    dag=dag
)

create_forecast_task = AwsForecastOperator(
    task_id='create_forecast',
    config={
        'ForecastName': 'electricityforecast_{{ ds_nodash }}',
        'PredictorArn': 'arn:aws:forecast:us-west-2:acct-id:predictor/electricitypredictor'
    },
    aws_conn_id='aws_default',
    dag=dag
)

wait_for_forecast_task = AwsForecastSensor(
    task_id='wait_for_forecast',
    forecast_arn="{{ task_instance.xcom_pull(task_ids='create_forecast')}}",
    aws_conn_id='aws_default',
    timeout=60 * 60,  # timeout(seconds) for Sensor polling (60 min)
    dag=dag
)

create_export_job_task = AwsForecastExportOperator(
    task_id='create_export_job',
    config={
        'ForecastExportJobName': 'electricity_forecast_export_job',
        'ForecastArn': "{{ task_instance.xcom_pull(task_ids='create_forecast')}}",
        'Destination': {
            'S3Config': {
                'Path': 's3://your-bucket/output_data/forecast',
                'RoleArn': 'arn:aws:iam::acct-id:role/forecast_role'
            }
        }
    },
    aws_conn_id='aws_default',
    dag=dag
)

wait_for_export_job_task = AwsForecastExportSensor(
    task_id='wait_for_export_job',
    export_job_arn="{{ task_instance.xcom_pull(task_ids='create_export_job')}}",
    aws_conn_id='aws_default',
    timeout=20 * 60,  # timeout(seconds) for Sensor polling (20 min)
    dag=dag
)

create_import_job_task >> wait_for_import_job_task >> create_forecast_task >> wait_for_forecast_task >> create_export_job_task >> wait_for_export_job_task
