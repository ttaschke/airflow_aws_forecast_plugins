# -*- coding: utf-8 -*-
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.aws_forecast_hook import AwsForecastHook

class AwsForecastImportSensor(BaseSensorOperator):
    """
    Sensor to poll for final status of an AWS Forecast dataset import job

    :param import_job_arn: The ARN of the dataset import job, which should be obtained via xcom from AwsForecastImportOperator
    :param aws_conn_id: Airflow connection with type 'aws'
    """

    NON_TERMINAL_STATES = ['CREATE_PENDING', 'CREATE_IN_PROGRESS']
    FAILED_STATES = ['CREATE_FAILED']

    template_fields = ['import_job_arn']

    @apply_defaults
    def __init__(self, import_job_arn, aws_conn_id='aws_default', *args, **kwargs):
        """
        :param str import_job_arn: The ARN of the dataset import job
        :param str aws_conn_id: Airflow connection with type 'aws'
        """
        super(AwsForecastImportSensor, self).__init__(*args, **kwargs)
        self.import_job_arn = import_job_arn
        self.aws_conn_id = aws_conn_id

    def poke(self, context):

        forecast_hook = AwsForecastHook(aws_conn_id=self.aws_conn_id)

        response = forecast_hook.describe_dataset_import_job(import_job_arn=self.import_job_arn)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f"Could not get dataset import job status: {response}")

        if response['Status'] in self.NON_TERMINAL_STATES:
            return False

        if response['Status'] in self.FAILED_STATES:
            raise AirflowException(f"Dataset import job failed: {response['Status']} {response['message']}")

        return True
