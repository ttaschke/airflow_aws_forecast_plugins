from airflow.contrib.hooks.aws_hook import AwsHook


class AwsForecastHook(AwsHook):
    """
    This hook handles all AWS Forecast API related actions
    :param aws_conn_id: Airflow connection with type 'aws'
    :param verify: Whether to verify SSL certificates for boto3 session configuration
    """

    def __init__(self, aws_conn_id='aws_default', verify=None):
        super().__init__(aws_conn_id, verify)

    def get_conn(self):
        """
        Initialize boto3 Amazon Forecast client

        :rtype: :py:class:`ForecastService.Client`
        """
        return self.get_client_type('forecast')

    def create_dataset_import_job(self, config):
        response = self.get_conn().create_dataset_import_job(**config)
        return response

    def describe_dataset_import_job(self, import_job_arn):
        response = self.get_conn().describe_dataset_import_job(DatasetImportJobArn=import_job_arn)
        return response

    def create_forecast(self, config):
        response = self.get_conn().create_forecast(**config)
        return response

    def describe_forecast(self, forecast_arn):
        response = self.get_conn().describe_forecast(ForecastArn=forecast_arn)
        return response

    def create_forecast_export_job(self, config):
        response = self.get_conn().create_forecast_export_job(**config)
        return response

    def describe_forecast_export_job(self, export_job_arn):
        response = self.get_conn().describe_forecast_export_job(ForecastExportJobArn=export_job_arn)
        return response
