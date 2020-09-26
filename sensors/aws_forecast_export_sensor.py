# -*- coding: utf-8 -*-
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.aws_forecast_hook import AwsForecastHook

class AwsForecastExportSensor(BaseSensorOperator):
    """
    Sensor to poll for final status of an AWS Forecast forecast export job

    :param export_job_arn: The ARN of the forecast export job, which should be obtained via xcom from AwsForecastExportOperator
    :param aws_conn_id: Airflow connection with type 'aws'
    """

    NON_TERMINAL_STATES = ['CREATE_PENDING', 'CREATE_IN_PROGRESS']
    FAILED_STATES = ['CREATE_FAILED']

    template_fields = ['export_job_arn']

    @apply_defaults
    def __init__(self, export_job_arn, aws_conn_id='aws_default', *args, **kwargs):
        """
        :param str export_job_arn: The ARN of the forecast export job
        :param str aws_conn_id: Airflow connection with type 'aws'
        """
        super(AwsForecastExportSensor, self).__init__(*args, **kwargs)
        self.export_job_arn = export_job_arn
        self.aws_conn_id = aws_conn_id

    def poke(self, context):

        forecast_hook = AwsForecastHook(aws_conn_id=self.aws_conn_id)

        response = forecast_hook.describe_forecast_export_job(export_job_arn=self.export_job_arn)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f"Could not get forecast export job status: {response}")

        if response['Status'] in self.NON_TERMINAL_STATES:
            return False

        if response['Status'] in self.FAILED_STATES:
            raise AirflowException(f"Forecast export job failed: {response['Status']} {response['message']}")

        return True
