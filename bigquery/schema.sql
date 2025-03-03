-- Create a dataset for the AQI project
CREATE SCHEMA IF NOT EXISTS `aqms_dataset`;

-- Table for historical AQI data
CREATE OR REPLACE TABLE `aqms_dataset.historical_aqi` (
  location_id STRING NOT NULL,
  location_name STRING NOT NULL,
  latitude FLOAT64 NOT NULL,
  longitude FLOAT64 NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  pm25 FLOAT64 NOT NULL,
  pm10 FLOAT64 NOT NULL,
  temperature FLOAT64 NOT NULL,
  humidity FLOAT64 NOT NULL,
  pm25_calibrated FLOAT64 NOT NULL,
  pm10_calibrated FLOAT64 NOT NULL,
  aqi_prediction FLOAT64 NOT NULL,
  aqi_category STRING NOT NULL,
  prediction_timestamp TIMESTAMP NOT NULL
) PARTITION BY DATE(timestamp);

-- Table for weather data
CREATE OR REPLACE TABLE `aqms_dataset.weather_data` (
  location_name STRING NOT NULL,
  latitude FLOAT64 NOT NULL,
  longitude FLOAT64 NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  temperature FLOAT64,
  humidity FLOAT64,
  wind_speed FLOAT64,
  wind_direction FLOAT64,
  precipitation FLOAT64,
  pressure FLOAT64,
  weather_condition STRING,
  weather_description STRING
) PARTITION BY DATE(timestamp);

-- Table for satellite data
CREATE OR REPLACE TABLE `aqms_dataset.satellite_data` (
  location_name STRING NOT NULL,
  latitude FLOAT64 NOT NULL,
  longitude FLOAT64 NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  no2 FLOAT64,
  co FLOAT64,
  o3 FLOAT64,
  so2 FLOAT64,
  pm25 FLOAT64,
  pm10 FLOAT64
) PARTITION BY DATE(timestamp);

-- View for aggregated daily AQI statistics
CREATE OR REPLACE VIEW `aqms_dataset.daily_aqi_stats` AS
SELECT
  location_name,
  DATE(timestamp) as date,
  AVG(aqi_prediction) as avg_aqi,
  MAX(aqi_prediction) as max_aqi,
  MIN(aqi_prediction) as min_aqi,
  AVG(pm25_calibrated) as avg_pm25,
  AVG(pm10_calibrated) as avg_pm10,
  AVG(temperature) as avg_temperature,
  AVG(humidity) as avg_humidity,
  COUNT(*) as num_readings
FROM
  `aqms_dataset.historical_aqi`
GROUP BY
  location_name, DATE(timestamp)
ORDER BY
  location_name, date DESC;

-- View for hourly AQI statistics
CREATE OR REPLACE VIEW `aqms_dataset.hourly_aqi_stats` AS
SELECT
  location_name,
  DATETIME(TIMESTAMP_TRUNC(timestamp, HOUR)) as hour,
  AVG(aqi_prediction) as avg_aqi,
  MAX(aqi_prediction) as max_aqi,
  MIN(aqi_prediction) as min_aqi,
  AVG(pm25_calibrated) as avg_pm25,
  AVG(pm10_calibrated) as avg_pm10,
  AVG(temperature) as avg_temperature,
  AVG(humidity) as avg_humidity,
  COUNT(*) as num_readings
FROM
  `aqms_dataset.historical_aqi`
GROUP BY
  location_name, hour
ORDER BY
  location_name, hour DESC;

-- View for weekly AQI statistics
CREATE OR REPLACE VIEW `aqms_dataset.weekly_aqi_stats` AS
SELECT
  location_name,
  DATE_TRUNC(DATE(timestamp), WEEK) as week_start,
  AVG(aqi_prediction) as avg_aqi,
  MAX(aqi_prediction) as max_aqi,
  MIN(aqi_prediction) as min_aqi,
  AVG(pm25_calibrated) as avg_pm25,
  AVG(pm10_calibrated) as avg_pm10,
  AVG(temperature) as avg_temperature,
  AVG(humidity) as avg_humidity,
  COUNT(*) as num_readings
FROM
  `aqms_dataset.historical_aqi`
GROUP BY
  location_name, week_start
ORDER BY
  location_name, week_start DESC;

-- Table for user feedback
CREATE OR REPLACE TABLE `aqms_dataset.user_feedback` (
  feedback_id STRING NOT NULL,
  user_id STRING,
  location_name STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  timestamp TIMESTAMP NOT NULL,
  rating INT64, -- User rating (e.g., 1-5 stars)
  comment STRING,
  feedback_type STRING, -- Type of feedback (e.g., "general", "air_quality", "app_experience")
  app_version STRING,
  device_info STRING
) PARTITION BY DATE(timestamp);