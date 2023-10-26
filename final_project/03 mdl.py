# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# Load necessary libraries
from pyspark.sql.functions import to_date, hour
import mlflow
import json
import pandas as pd
import numpy as np
from prophet import Prophet, serialize
from prophet.diagnostics import cross_validation, performance_metrics

# Visualization
import seaborn as sns
import matplotlib.pyplot as plt

# Hyperparameter tuning
import itertools

# COMMAND ----------

ARTIFACT_PATH = "G04-model"
np.random.seed(12345)

# COMMAND ----------

## Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# COMMAND ----------

# DBTITLE 1,Using target_variable table created during etl process
target_data=spark.sql("select * from target_variable")
display(target_data.head(2))

# COMMAND ----------

target_df = target_data.toPandas()

# COMMAND ----------

target_df.info()

# COMMAND ----------

target_df=target_df.astype(int)

# COMMAND ----------

# Combine year, month, and date columns to create a datetime column
target_df['datetime'] = target_df.apply(lambda x: pd.to_datetime(f"{x['dateofmonth_sa']}-{x['monthofyr_sa']}-{x['year_sa']}-{x['hourofday_sa']}", format="%d-%m-%Y-%H"), axis=1)

# Print the updated dataframe
display(target_df.head(2))


# COMMAND ----------

max(target_df['netchange'])

# COMMAND ----------

# DBTITLE 1,Visualize training set using seaborn
sns.set(rc={'figure.figsize':(12,8)})
sns.lineplot(x=target_df['datetime'], y=target_df['netchange'])
plt.legend(['Bike Rides Net Change'])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#Renaming columns for fb prophet
df_grouped = target_df.rename(columns={"datetime": "ds", "netchange": "y"})
display(df_grouped)

# COMMAND ----------

# Baseline Model Using Default Hyperparameters
# - Horizon - period over which we forecast
# - Initial - amount of initial training data
# - Period - time between cutoffs (usually H/2)
# - Cutoff - beginning of the Horizon forecast period
#--------------------------------------------#

# Initiate the model
baseline_model = Prophet()

# Fit the model on the training dataset
baseline_model.fit(df_grouped)

# Cross validation
baseline_model_cv = cross_validation(model=baseline_model, initial='200 days', period='60 days',horizon = '120 hours', parallel="threads")
baseline_model_cv.head()

# Model performance metrics
baseline_model_p = performance_metrics(baseline_model_cv, rolling_window=1)
baseline_model_p.head()

# Get the performance value
print(f"RMSE of baseline model: {baseline_model_p['rmse'].values[0]}")

# COMMAND ----------

# DBTITLE 1,Automatic Hyperparameter Tuning
#--------------------------------------------#
# Automatic Hyperparameter Tuning
#--------------------------------------------#

# Set up parameter grid
param_grid = {  
    'changepoint_prior_scale': [0.01],  # , 0.05, 0.08, 0.5
    'seasonality_prior_scale': [0.1],  # , 1, 5, 10, 12
    'seasonality_mode': ['additive', 'multiplicative']
}
  
# Generate all combinations of parameters
all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]

print(f"Total training runs {len(all_params)}")

# Create a list to store MAPE values for each combination
mapes = [] 

# Use cross validation to evaluate all parameters
for params in all_params:
    with mlflow.start_run(): 
        # Fit a model using one parameter combination + holidays
        m = Prophet(**params) 
        holidays = pd.DataFrame({"ds": [], "holiday": []})
        #m.add_country_holidays(country_name='US')
        m.fit(df_grouped) 

        # Cross-validation
        df_cv = cross_validation(model=m, initial='200 days', period='60 days', horizon = '120 days', parallel="threads")
        # Model performance
        df_p = performance_metrics(df_cv, rolling_window=1)
        print(df_p.columns)

        metric_keys = ["mse", "rmse", "mae", "mdape", "smape", "coverage"]
        metrics = {k: df_p[k].mean() for k in metric_keys}
        params = extract_params(m)

        print(f"Logged Metrics: \n{json.dumps(metrics, indent=2)}")
        print(f"Logged Params: \n{json.dumps(params, indent=2)}")

        mlflow.prophet.log_model(m, artifact_path=ARTIFACT_PATH)
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        model_uri = mlflow.get_artifact_uri(ARTIFACT_PATH)
        print(f"Model artifact logged to: {model_uri}")

        # Save model performance metrics for this combination of hyper parameters
        mapes.append((df_p['rmse'].values[0],model_uri))
        

# COMMAND ----------

# Tuning results
tuning_results = pd.DataFrame(all_params)
tuning_results['rmse'] = list(zip(*mapes))[0]
tuning_results['model']= list(zip(*mapes))[1]

best_params = dict(tuning_results.iloc[tuning_results[['rmse']].idxmin().values[0]])

print(json.dumps(best_params, indent=2))

# COMMAND ----------

# DBTITLE 1,Loading model and predicting for random #hours
loaded_model = mlflow.prophet.load_model(best_params['model'])

forecast = loaded_model.predict(loaded_model.make_future_dataframe(879, freq="h"))

print(f"forecast:\n${forecast.tail(40)}")

# COMMAND ----------

prophet_plot = loaded_model.plot(forecast)

# COMMAND ----------

prophet_plot2 = loaded_model.plot_components(forecast)

# COMMAND ----------

# DBTITLE 1,Preparing actual data (bike station)
# lag function is used to create netchange for actual data
spark.sql("drop table if exists actual_data_forecast")
spark.sql("create table if not exists actual_data_forecast as select a.hourofday,a.dateofmonth,a.year,a.monthofyr,a.last_reported_datetime,a.num_bikes_available,coalesce(LAG(a.num_bikes_available) OVER (ORDER BY a.last_reported_datetime ASC),0) AS lag_num_bikes_available from  (select last_reported_datetime,minute(last_reported_datetime) as min_time,hourofday,dateofmonth,year,monthofyr,num_docks_available,num_docks_disabled,num_bikes_disabled,num_bikes_available from silver_station_status_dynamic where year=2023 and monthofyr>3) as a where min_time<30")

# COMMAND ----------

display(spark.sql("select * from actual_data_forecast limit 2"))

# COMMAND ----------

df1=spark.sql("select *,(num_bikes_available-lag_num_bikes_available) as netchange from actual_data_forecast")
display(df1.head(2))

# COMMAND ----------

df1=df1.toPandas()
df1['ds'] = df1.apply(lambda x: pd.to_datetime(f"{x['dateofmonth']}-{x['monthofyr']}-{x['year']}-{x['hourofday']}", format="%d-%m-%Y-%H"), axis=1)
display(df1.head(2))

# COMMAND ----------

forecast['ds_date'] = forecast['ds'].apply(lambda x: x.date())
# forecast.to_frame()
forecast.head(2)

# COMMAND ----------

forecast['ds_date'] = pd.to_datetime(forecast['ds_date'], errors='coerce')

# COMMAND ----------

# picking forecast data post Apr 1'2023
forecast_residual = forecast[forecast['ds_date'] > "2023-03-31"]
forecast_residual.shape

# COMMAND ----------

max(forecast_residual["ds_date"])

# COMMAND ----------

forecast_v1=forecast_residual.merge(df1[['ds','netchange']],how='left',on='ds')
display(forecast_v1.head(10))

# COMMAND ----------

forecast_v1.shape

# COMMAND ----------

# DBTITLE 1,Create a residual plot by joining training data with forecast
results=forecast[['ds','yhat']].join(df1, lsuffix='_caller', rsuffix='_other')
forecast_v1['residual'] = forecast_v1['yhat'] - forecast_v1['netchange']

# COMMAND ----------

# plot the residuals
import plotly.express as px
fig = px.scatter(
    forecast_v1, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols',
)
fig.show()

# COMMAND ----------

# DBTITLE 1,Register the best model and move it into staging
model_details = mlflow.register_model(model_uri=best_params['model'], name=ARTIFACT_PATH)

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()

# COMMAND ----------

# DBTITLE 1,Transitioning model to next stage
client.transition_model_version_stage(

  name=model_details.name,

  version=model_details.version,

  stage='Staging',

)

# COMMAND ----------

# DBTITLE 1,Check current model stage
model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

# DBTITLE 1,Check latest staging version for Production & Staging
latest_version_info_staging = client.get_latest_versions(ARTIFACT_PATH, stages=["Staging"])
latest_version_info_production = client.get_latest_versions(ARTIFACT_PATH, stages=["Production"])

latest_production_version = latest_version_info_production[0].version
latest_staging_version = latest_version_info_staging[0].version
#print(latest_version_info)
print("The latest production version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_production_version))
print("The latest staging version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_staging_version))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
