# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 1,Read stream for bike and weather historic csv data

from pyspark.sql.functions import *
from pyspark.sql.types import *

bike_for_schema = spark.read.csv(BIKE_TRIP_DATA_PATH,sep=",",header="true")
weather_for_schema = spark.read.csv(NYC_WEATHER_FILE_PATH,sep=",",header="true")


# Read Stream One-time-operation done 
'''
bike_trip_data = spark \
  .readStream \
  .schema(bike_for_schema.schema) \
  .option("maxFilesPerTrigger", 1) \
  .option("append","true") \
  .csv(BIKE_TRIP_DATA_PATH)

weather_data = spark \
  .readStream \
  .schema(weather_for_schema.schema) \
  .option("maxFilesPerTrigger", 1) \
  .option("append","true") \
  .csv(NYC_WEATHER_FILE_PATH)
  '''


# COMMAND ----------

# DBTITLE 1,Read bronze data files and create dynamic data frames
station_info_all = spark.read.format("delta").load(BRONZE_STATION_INFO_PATH)
station_status_all = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH)
weather_dynamic_all = spark.read.format("delta").load(BRONZE_NYC_WEATHER_PATH)

# COMMAND ----------

# DBTITLE 1,Creating station info table for our station name
station_info=station_info_all.filter(station_info_all["name"]==GROUP_STATION_ASSIGNMENT)
display(station_info)

# COMMAND ----------

# DBTITLE 1,Creating station status table for our station name
group_id=station_info.select("station_id").collect()[0][0]
station_status=station_status_all.filter(station_status_all["station_id"]==group_id)
display(station_status)

# COMMAND ----------

# DBTITLE 1,Creating dynamic tables
station_status.write.saveAsTable("G04_db.bronze_station_status_dynamic", format='delta', mode='overwrite')
station_info.write.saveAsTable("G04_db.bronze_station_info_dynamic", format='delta', mode='overwrite')
weather_dynamic_all.write.partitionBy("time").option("overwriteSchema", "true").saveAsTable("G04_db.bronze_weather_info_dynamic", format='delta', mode='overwrite')

# COMMAND ----------

# DBTITLE 1,Remove files from group location path
# to remove files from directory
# dbutils.fs.rm('dbfs:/FileStore/tables/G04/bike_trip_data/',True)

# COMMAND ----------

# DBTITLE 1,Write Stream to append bike trips data
# Write Stream data One-Time-Operation perfomred 

'''
bike_trip_data.writeStream.format("delta")\
  .outputMode("append")\
  .option("checkpointLocation","dbfs:/FileStore/tables/G04/bike_trip_data/checkpoint")\
  .start("dbfs:/FileStore/tables/G04/bike_trip_data/")


bike_stream = spark.read.format("delta").load("dbfs:/FileStore/tables/G04/bike_trip_data/")
bike_stream.write.format("delta").mode("overwrite").saveAsTable("g04_db.bronze_bike_trip_historic")
'''

# COMMAND ----------

# DBTITLE 1,Show files under group file path for bike trip historic data
display(dbutils.fs.ls("dbfs:/FileStore/tables/G04/bike_trip_data"))

# COMMAND ----------

# Validation
display(spark.sql('select max(started_at) from g04_db.bronze_bike_trip_historic where started_at!="started_at"'))

# COMMAND ----------

# Validation
display(spark.sql('select count(*) from g04_db.bronze_bike_trip_historic where started_at!="started_at"'))

# COMMAND ----------

# DBTITLE 1,Write Stream to append weather data
# Write Stream data One-Time-Operation perfomred 

'''
weather_data.writeStream.format("delta")\
  .outputMode("append")\
  .option("checkpointLocation","dbfs:/FileStore/tables/G04/weather_data/checkpoint")\
  .start("dbfs:/FileStore/tables/G04/weather_data/")

weather_stream = spark.read.format("delta").load("dbfs:/FileStore/tables/G04/weather_data/")
weather_stream.write.format("delta").mode("overwrite").saveAsTable("g04_db.bronze_weather_historic")
'''

# COMMAND ----------

# DBTITLE 1,Show files under group file path for weather historic data
display(dbutils.fs.ls("dbfs:/FileStore/tables/G04/weather_data"))

# COMMAND ----------

# Validation
display(spark.sql('select count(*) from g04_db.bronze_weather_historic'))

# COMMAND ----------

# Get tables
display(spark.sql('show tables'))

# COMMAND ----------

display(spark.sql('select * from silver_station_status_dynamic'))

# COMMAND ----------

# DBTITLE 1, Creating silver tables
# MAGIC %md

# COMMAND ----------

# Creating silver table for bike status info dynamic table
display(spark.sql("drop table if exists silver_station_status_dynamic"))
display(spark.sql('CREATE TABLE if not exists silver_station_status_dynamic as select a.*,hour(last_reported_datetime) as hourofday, day(last_reported_datetime) as dateofmonth, dayofyear(last_reported_datetime) as dateofyear,month(last_reported_datetime) as monthofyr, year(last_reported_datetime) as year from (select *,to_timestamp(last_reported) as last_reported_datetime from bronze_station_status_dynamic) as a'))

# COMMAND ----------

# Creating silver table for bike status info dynamic table
display(spark.sql("create table if not exists silver_station_status_dynamic_v1 as select *,date_format(last_reported_datetime,'EEEE') as weekday  from silver_station_status_dynamic"))
spark.sql('drop table if exists silver_station_status_dynamic')
spark.sql('create table if not exists silver_station_status_dynamic as select * from silver_station_status_dynamic_v1')
spark.sql('drop table if exists silver_station_status_dynamic_v1')

# COMMAND ----------

# Creating silver table for bike trip historic table
display(spark.sql("drop table if exists silver_bike_trip_historic"))
display(spark.sql('CREATE TABLE if not exists silver_bike_trip_historic as select *,hour(started_at) as hourofday_sa, day(started_at) as dateofmonth_sa, dayofyear(started_at) as dateofyear_sa,month(started_at) as monthofyr_sa, year(started_at) as year_sa,hour(ended_at) as hourofday_ea, day(ended_at) as dateofmonth_ea, dayofyear(ended_at) as dateofyear_ea,month(ended_at) as monthofyr_ea, year(ended_at) as year_ea from (select * from bronze_bike_trip_historic where started_at!="started_at")'))

# COMMAND ----------

# Creating silver table for bike trip historic table
display(spark.sql("create table if not exists silver_bike_trip_historic_v1 as select *,date_format(started_at,'EEEE') as weekday_startdate,date_format(ended_at,'EEEE') as weekday_enddate  from silver_bike_trip_historic"))
spark.sql('drop table if exists silver_bike_trip_historic')
spark.sql('create table if not exists silver_bike_trip_historic as select * from silver_bike_trip_historic_v1')
spark.sql('drop table if exists silver_bike_trip_historic_v1')

# COMMAND ----------

# Creating silver table for weather info dynamic table
display(spark.sql("drop table if exists silver_weather_info_dynamic"))
display(spark.sql('create table if not exists silver_weather_info_dynamic as select a.*,final_weather.description as weather_description,final_weather.icon as weather_icon,final_weather.id as weather_id,final_weather.main as weather_main, hour(time) as hourofday, day(time) as dateofmonth, dayofyear(time) as dateofyear,month(time) as monthofyr, year(time) as year from (select *,explode(weather) as final_weather from bronze_weather_info_dynamic) as a '))

# COMMAND ----------

# Creating silver table for weather info dynamic table
display(spark.sql("create table if not exists silver_weather_info_dynamic_v1 as select *,date_format(time,'EEEE') as weekday  from silver_weather_info_dynamic"))
spark.sql('drop table if exists silver_weather_info_dynamic')
spark.sql('create table if not exists silver_weather_info_dynamic as select * from silver_weather_info_dynamic_v1')
spark.sql('drop table if exists silver_weather_info_dynamic_v1')

# COMMAND ----------

# Creating silver table for weather historic table
display(spark.sql("drop table if exists silver_weather_historic"))
display(spark.sql('CREATE TABLE if not exists silver_weather_historic as select a.*,hour(last_reported_datetime) as hourofday, day(last_reported_datetime) as dateofmonth, dayofyear(last_reported_datetime) as dateofyear,month(last_reported_datetime) as monthofyr, year(last_reported_datetime) as year from (select float(temp), float(feels_like), float(pressure), float(humidity), float(dew_point), float(uvi), float(clouds), float(visibility), float(wind_speed), float(wind_deg), float(pop), float(snow_1h),to_timestamp(int(dt)) as last_reported_datetime,main,description,icon,lat,lon,timezone,timezone_offset,rain_1h from bronze_weather_historic where dt!="dt") as a'))

# COMMAND ----------

# Creating silver table for weather historic table
display(spark.sql("create table if not exists silver_weather_historic_v1 as select *,date_format(last_reported_datetime,'EEEE') as weekday  from silver_weather_historic"))
spark.sql('drop table if exists silver_weather_historic')
spark.sql('create table if not exists silver_weather_historic as select * from silver_weather_historic_v1')
spark.sql('drop table if exists silver_weather_historic_v1')

# COMMAND ----------

# Checking count of rows in each silver table
display(spark.sql('select "silver_bike_trip_historic" as tablename,count(*) as rows from silver_bike_trip_historic union select "silver_station_status_dynamic" as tablename,count(*) as rows from silver_station_status_dynamic union select "silver_weather_historic" as tablename,count(*) as rows from silver_weather_historic union select "silver_weather_info_dynamic" as tablename,count(*) as rows from silver_weather_info_dynamic'))


# COMMAND ----------

# DBTITLE 1,Optimization Step : Z-Ordering Tables
# None of these tables have partition column therefore, applying z-ordering for optimization
display(spark.sql('OPTIMIZE silver_bike_trip_historic ZORDER BY (started_at,ended_at)'))
display(spark.sql('OPTIMIZE silver_station_status_dynamic ZORDER BY (last_reported_datetime)'))
display(spark.sql('OPTIMIZE silver_weather_historic ZORDER BY (last_reported_datetime)'))

# COMMAND ----------

# DBTITLE 1,Creating target variable for model training
display(spark.sql('drop table if exists target_variable_v1'))

spark.sql('create table if not exists target_variable_v1 as select a.year_sa,a.monthofyr_sa,a.dateofmonth_sa,a.hourofday_sa,b.year_ea,b.monthofyr_ea,b.dateofmonth_ea,b.hourofday_ea,coalesce(a.rides_started,0) as rides_started,coalesce(b.rides_ended,0) as rides_ended,(coalesce(b.rides_ended,0)-coalesce(a.rides_started,0)) as netchange from (select year_sa,monthofyr_sa,dateofmonth_sa,hourofday_sa,count(distinct ride_id) as rides_started from silver_bike_trip_historic where start_station_name="6 Ave & W 33 St"  and started_at<="2023-03-31 23:59:57" group by year_sa,monthofyr_sa,dateofmonth_sa,hourofday_sa) as a FULL OUTER join (select year_ea,monthofyr_ea,dateofmonth_ea,hourofday_ea,count(distinct ride_id) as rides_ended from silver_bike_trip_historic where end_station_name="6 Ave & W 33 St" and ended_at<="2023-03-31 23:59:57" group by year_ea,monthofyr_ea,dateofmonth_ea,hourofday_ea) as b on a.year_sa=b.year_ea and a.monthofyr_sa=b.monthofyr_ea and a.dateofmonth_sa=b.dateofmonth_ea and a.hourofday_sa=b.hourofday_ea')

# COMMAND ----------

netchange = spark.sql('select * from target_variable_v1')
netchange = netchange.toPandas()

netchange['year_sa'].fillna(netchange['year_ea'], inplace=True)
netchange['monthofyr_sa'].fillna(netchange['monthofyr_ea'], inplace=True)
netchange['dateofmonth_sa'].fillna(netchange['dateofmonth_ea'], inplace=True)
netchange['hourofday_sa'].fillna(netchange['hourofday_ea'], inplace=True)

display(netchange.isna().any())

# COMMAND ----------

netchange.drop('year_ea', inplace=True, axis = 1)
netchange.drop('monthofyr_ea', inplace=True, axis = 1)
netchange.drop('dateofmonth_ea', inplace=True, axis = 1)
netchange.drop('hourofday_ea', inplace=True, axis = 1)


# COMMAND ----------

display(spark.sql('drop table if exists target_variable'))
netchange_spark = spark.createDataFrame(netchange)
netchange_spark.write.saveAsTable("G04_db.target_variable", format='delta', mode='overwrite')

display(spark.sql("SELECT * FROM target_variable LIMIT 20"))

# COMMAND ----------

# DBTITLE 1,Validation steps
# Validation - Checking target variable data for model training 
display(spark.sql('select * from silver_bike_trip_historic where (start_station_name="6 Ave & W 33 St" or end_station_name="6 Ave & W 33 St") and year_sa=2021 and monthofyr_sa=11 and dateofmonth_sa=1 and (hourofday_sa=0 or hourofday_ea=0) and year_ea=2021 and monthofyr_ea=11 and dateofmonth_ea=1'))

# COMMAND ----------

#Validation - No null records
display(spark.sql('select max(netchange) from target_variable '))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
