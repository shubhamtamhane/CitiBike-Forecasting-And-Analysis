# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 1,Define Widgets
dbutils.widgets.dropdown("03.hours_to_forecast", "48", ["24", "48", "72", "96"])
dbutils.widgets.dropdown("04.frequency_to_forecast", "h", ["h", "d", "m"])
dbutils.widgets.dropdown("05.promote_model", "Yes", ["Yes","No"])

# COMMAND ----------

# DBTITLE 1,Get widgets
#start_date = str(dbutils.widgets.get('01.start_date'))
#end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
frequency_to_forecast = str(dbutils.widgets.get('04.frequency_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('05.promote_model')).lower() == 'yes' else False)

print("Hours to forecast:",hours_to_forecast)
print("Frequency of forecast:",frequency_to_forecast)
print("Do you want to promote the existing staging model:",promote_model)

# COMMAND ----------

# DBTITLE 1,Remove widgets
# dbutils.widgets.removeAll()

# COMMAND ----------

import numpy as np
ARTIFACT_PATH = "G04-model"
np.random.seed(12345)

# COMMAND ----------

pip install folium

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()

# COMMAND ----------

# DBTITLE 1,Check latest staging version for Production & Staging
ARTIFACT_PATH = "G04-model"

latest_version_info_staging = client.get_latest_versions(ARTIFACT_PATH, stages=["Staging"])
latest_version_info_production = client.get_latest_versions(ARTIFACT_PATH, stages=["Production"])

latest_production_version = latest_version_info_production[0].version
latest_staging_version = latest_version_info_staging[0].version
#print(latest_version_info)
print("The latest production version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_production_version))
print("The latest staging version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_staging_version))

# COMMAND ----------

# DBTITLE 1,Load registered model from staging
import mlflow
model_stag_uri = "models:/{model_name}/staging".format(model_name=ARTIFACT_PATH)

print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_stag_uri))

model_stag = mlflow.prophet.load_model(model_stag_uri)

# COMMAND ----------

# DBTITLE 1,Transition: Staging -> Production
model_name="G04-model"
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
frequency_to_forecast = str(dbutils.widgets.get('04.frequency_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('05.promote_model')).lower() == 'yes' else False)


if promote_model is True:
    client.transition_model_version_stage(
    name=model_name,
    version=latest_staging_version,
    stage='Production')
    print("The model has been transitioned to production")
else:
    print("You have chosen not to promote the model")

# COMMAND ----------

# DBTITLE 1,Transition: Production -> Archived

if promote_model is True:
    client.transition_model_version_stage(
    name=model_name,
    version=latest_production_version,
    stage='Archived')
    print("The model has been transitioned to archived")
else:
    print("The model version does not exist")

# COMMAND ----------

# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 1,City Bike Station Trip Data & Current Weather
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
 
# Creating a spark session
spark_session = SparkSession.builder.appName(
    'Spark_Session').getOrCreate()
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('Value', StringType(), True),
  StructField('Category', StringType(), True)])
df=spark.createDataFrame([],schema)

# Current weather info
current_weather=spark.sql("select temp,pop,feels_like,humidity,current_timestamp() as current_time from silver_weather_info_dynamic order by time desc limit 1")

# Current bike station status info
bike_station=spark.sql("select num_ebikes_available,num_docks_available,num_scooters_available,num_bikes_available,num_bikes_disabled,num_docks_disabled from silver_station_status_dynamic order by last_reported_datetime desc limit 1")

rows = [["Current Time",str(current_weather.select("current_time").collect()[0][0])],
["Station Name",GROUP_STATION_ASSIGNMENT],
["Production Model Version",latest_production_version],
["Staging Model Version",latest_staging_version],
["Current Temp",str(current_weather.select("temp").collect()[0][0])],
["Current Pop",str(current_weather.select("pop").collect()[0][0])],
["Current Humidity",str(current_weather.select("humidity").collect()[0][0])],
["Total Docks",str(52)],
["Ebikes Available",str(bike_station.select("num_ebikes_available").collect()[0][0])],
["Docks Available",str(bike_station.select("num_docks_available").collect()[0][0])],
["Scooters Available",str(bike_station.select("num_scooters_available").collect()[0][0])],
["Bikes Available",str(bike_station.select("num_bikes_available").collect()[0][0])],
["Bikes Disabled",str(bike_station.select("num_bikes_disabled").collect()[0][0])],
["Docks Disabled",str(bike_station.select("num_docks_disabled").collect()[0][0])]]
columns = ['Category','Value']
 
# Creating the DataFrame
second_df = spark_session.createDataFrame(rows, columns)

first_df = df.union(second_df)
display(first_df)

# COMMAND ----------

# DBTITLE 1,Interactive Map to show station location and name
import folium
# Plot Gaussian means (cluster centers):
center_map = folium.Map(location=[40.74901271, -73.98848395], zoom_start=13,title="Tanvi")
iframe = folium.IFrame(GROUP_STATION_ASSIGNMENT, width=150, height=25)
folium.Marker(location =[40.74901271, -73.98848395],fill_color='#43d9de').add_child(folium.Popup(iframe)).add_to(center_map)

html_string = center_map._repr_html_()

# Display the map 
displayHTML(html_string)


# COMMAND ----------

# DBTITLE 1,Read weather dynamic table to get the forecasted weather data
forecasted_weather=spark.sql("select temp,time from silver_weather_info_dynamic where monthofyr=4 and dateofmonth between 28 and 29 ")
display(forecasted_weather.count())


# COMMAND ----------

# DBTITLE 1,Load registered model from Production
import mlflow
model_prod_uri = "models:/{model_name}/production".format(model_name=ARTIFACT_PATH)

print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_prod_uri))

model_production = mlflow.prophet.load_model(model_prod_uri)

# COMMAND ----------

from datetime import datetime

current_ts = datetime.now()
print(current_ts)

# COMMAND ----------

# This code gives hours till now starting from 1st Apr'2023
import pandas
df = pandas.DataFrame(columns=['to','fr','diff'])
df['to'] = [pandas.Timestamp('2023-04-01 00:00:00.000000')]
df['fr'] = [current_ts]
df['diff']=(df.fr-df.to).astype('timedelta64[h]')
diff=df.iloc[0,2]


# COMMAND ----------

# DBTITLE 1,Forecast for next n hours
hours=int(diff)+int(hours_to_forecast) # creating dynamic input for hours using widget outputs
forecasted_df=model_production.predict(model_production.make_future_dataframe(hours, freq=frequency_to_forecast))
model_production.plot(model_production.predict(model_production.make_future_dataframe(hours, freq=frequency_to_forecast)))

# COMMAND ----------

display(forecasted_df.head(2))

# COMMAND ----------

# DBTITLE 1,Picking the forecasted data from Apr 1'2023 onwards and creating residuals
forecasted_df['ds_date'] = forecasted_df['ds'].apply(lambda x: x.date())

forecasted_df['ds_date'] = pd.to_datetime(forecasted_df['ds_date'], errors='coerce')
forecast_residual = forecasted_df[forecasted_df['ds_date'] > "2023-03-31"]
forecast_residual.shape

# COMMAND ----------

# DBTITLE 1,Use actual data created in ML notebook using bike station info silver tables

df1=spark.sql("select *,(num_bikes_available-lag_num_bikes_available) as netchange from actual_data_forecast")

df1=df1.toPandas()
df1['ds'] = df1.apply(lambda x: pd.to_datetime(f"{x['dateofmonth']}-{x['monthofyr']}-{x['year']}-{x['hourofday']}", format="%d-%m-%Y-%H"), axis=1)
display(df1.head(2))

# COMMAND ----------

# DBTITLE 1,Adding Residuals
forecast_v1=forecast_residual.merge(df1[['ds','netchange']],how='left',on='ds')
display(forecast_v1.head(10))

#results=forecast[['ds','yhat']].join(df1, lsuffix='_caller', rsuffix='_other')
forecast_v1['residual'] = forecast_v1['yhat'] - forecast_v1['netchange']

# COMMAND ----------

display(forecast_v1.head(2))

# COMMAND ----------

# DBTITLE 1,Preparing bikes available for next 48 hours
from datetime import datetime

current_ts = datetime.now()
forecast_after_now = forecast_v1[forecast_v1['ds'] > current_ts]
forecast_after_now=forecast_after_now[['ds','yhat']]
print(current_ts)
display(forecast_after_now.head(2))
#display(forecast_after_now['ds'].max())

# COMMAND ----------

# DBTITLE 1,Get bikes available for latest timestamp given
max_bikes=spark.sql("select num_bikes_available from silver_station_status_dynamic order by last_reported_datetime desc limit 1")
no_bikes_available=max_bikes.collect()[0][0]
display(no_bikes_available)

# COMMAND ----------

# DBTITLE 1,Cumulative sum of yhat to get bikes available
forecast_after_now['bikes_available']=forecast_after_now['yhat'].cumsum()+no_bikes_available # no of bikes available for the latest timestamp + cumulaive sum of yhat (since it is netchange) gave us bikes available
forecast_after_now['capacity']=52
forecast_after_now.head(2)

# COMMAND ----------

# DBTITLE 1,Insert forecasted data to gold table
from pyspark.sql import SparkSession
forecast_pyspark = spark.createDataFrame(forecast_v1)

forecast_pyspark.write.saveAsTable("G04_db.gold_bike_forecast", format='delta', mode='overwrite')

# COMMAND ----------

# DBTITLE 1,Visualizations - Forecasted Results + Residual Plot
import plotly.express as px


fig = px.line(forecast_after_now, x='ds', y='bikes_available', title='Forecasted No. of bikes (Station - 6 Ave & W 33 St)')
fig.add_scatter(x=forecast_after_now['ds'], y=forecast_after_now['capacity'])
fig.add_annotation(dict(font=dict(color='black',size=15),x=0.8,y=0.85,showarrow=False,text="Station Capacity - 52",textangle=0,xanchor='left',xref="paper",yref="paper"))
#fig.add_shape(type="circle",
#    xref="x domain", yref="y domain",
#    x0=0.675, x1=0.715, y0=0.65, y1=0.8,
#)
fig.show()

# COMMAND ----------

import plotly.express as px
fig = px.scatter(
    forecast_v1, x='yhat',y='residual',opacity=0.65,title='Forecast Model Performance Comparison (Station - 6 Ave & W 33 St)',
    trendline='ols', trendline_color_override='darkblue',marginal_y='violin'
)
fig.show()

#fig = px.scatter(
#    forecast_v1, x='yhat', y='residual',
#    marginal_y='violin',
#    trendline='ols',
#)
#fig.show()

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
