# AWS Forecast plugins for Airflow

This repository contains plugins that allow easy operation of scheduled AWS Forecast (https://aws.amazon.com/forecast) production workloads. 

It is recommended to use this to operationalize AWS Forecast workloads that were preapared manually beforehand, including the creation of the predictor, accuracy tuning and other pre-production tasks. Airflow with this plugins can than be used to schedule daily (or other frequency) import of input data, predicting the forecast horizon and exporting the resulting data.

### Example usage

* `dag/sample_dag`

This example follows the AWS Foreacast `Getting Started` tutorial (https://docs.aws.amazon.com/forecast/latest/dg/getting-started.html) and requires the same preparations in regards to IAM permissions and S3.

Additionally, the following steps as described in the `Getting Started` tutorial are assumed to be prepared:

* Upload of training data `electricityusagedata.csv` to S3
* Create the corresponding AWS Forecast dataset
* Create a AWS Forecast Predictor

### Plugins overview

* Operators
  * AwsForecastImportOperator
     * Starts a dataset import job
  * AwsForecastOperator
     * Creates a forecast
  * AwsForecastExportOperator
     * Starts a forecast export job
* Sensors
  * AwsForecastImportSensor
     * Polls for the final status of a dataset import job
  * AwsForecastSensor
     * Polls for the final status of a forecast
  * AwsForecastExportSensor
     * Polls for the final status of a forecast result export job 
