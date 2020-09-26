from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.aws_forecast_hook import AwsForecastHook

class AwsForecastExportOperator(BaseOperator):
    """
    Operator to create an export job for AWS Forecast results

    :param config: The configuration for creating an export job (see boto3 ForecastService).
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

        response = forecast_hook.create_forecast_export_job(config=self.config)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f"Creating export job failed: {response}")
        else:
            return response['ForecastExportJobArn']
