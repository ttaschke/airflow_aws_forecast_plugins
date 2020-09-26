from airflow.plugins_manager import AirflowPlugin
from hooks.aws_forecast_hook import AwsForecastHook
from operators.aws_forecast_import_operator import AwsForecastImportOperator
from operators.aws_forecast_operator import AwsForecastOperator
from operators.aws_forecast_export_operator import AwsForecastExportOperator
from sensors.aws_forecast_import_sensor import AwsForecastImportSensor
from sensors.aws_forecast_sensor import AwsForecastSensor
from sensors.aws_forecast_export_sensor import AwsForecastExportSensor

class AirflowCustomPlugin(AirflowPlugin):
    name = "AirflowCustomPlugin"
    operators = [AwsForecastImportOperator, AwsForecastOperator, AwsForecastExportOperator]
    sensors = [AwsForecastImportSensor, AwsForecastSensor, AwsForecastExportSensor]
    hooks = [AwsForecastHook]
