# -*- coding: utf-8 -*-
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.aws_forecast_hook import AwsForecastHook

class AwsForecastSensor(BaseSensorOperator):
    """
    Sensor to poll for final status of an AWS Forecast forecast

    :param forecast_arn: The ARN of the forecast, which should be obtained via xcom from AwsForecastOperator
    :param aws_conn_id: Airflow connection with type 'aws'
    """

    NON_TERMINAL_STATES = ['CREATE_PENDING', 'CREATE_IN_PROGRESS']
    FAILED_STATES = ['CREATE_FAILED']

    template_fields = ['forecast_arn']

    @apply_defaults
    def __init__(self, forecast_arn, aws_conn_id='aws_default', *args, **kwargs):
        """
        :param str forecast_arn: The ARN of the forecast
        :param str aws_conn_id: Airflow connection with type 'aws'
        """
        super(AwsForecastSensor, self).__init__(*args, **kwargs)
        self.forecast_arn = forecast_arn
        self.aws_conn_id = aws_conn_id

    def poke(self, context):

        forecast_hook = AwsForecastHook(aws_conn_id=self.aws_conn_id)

        response = forecast_hook.describe_forecast(forecast_arn=self.forecast_arn)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f"Could not get forecast status: {response}")

        if response['Status'] in self.NON_TERMINAL_STATES:
            return False

        if response['Status'] in self.FAILED_STATES:
            raise AirflowException(f"Forecast failed: {response['Status']} {response['message']}")

        return True
