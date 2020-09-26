from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.aws_forecast_hook import AwsForecastHook

class AwsForecastOperator(BaseOperator):
    """
    Operator to create a forecast with AWS Forecast

    :param config: The configuration for creating a forecast (see boto3 ForecastService).
    :param aws_conn_id: Airflow connection with type 'aws'
    """
    template_fields = ['config']

    @apply_defaults
    def __init__(self, config, aws_conn_id='aws_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.config = config

    def execute(self, context):
        forecast_hook = AwsForecastHook(aws_conn_id=self.aws_conn_id)

        response = forecast_hook.create_forecast(config=self.config)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f"Creating forecast failed: {response}")
        else:
            return response['ForecastArn']
