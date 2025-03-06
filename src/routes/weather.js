const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const { Firestore } = require('@google-cloud/firestore');
const axios = require('axios');
const config = require('../config');

const router = express.Router();
const bigquery = new BigQuery(config.google);
const firestore = new Firestore(config.google);

// Get current weather for a specific location
router.get('/current/:locationId', async (req, res, next) => {
  try {
    const locationId = req.params.locationId;
    
    // Get location data from Firestore
    const locationDoc = await firestore.collection(config.firestore.collections.locations)
      .doc(locationId).get();
    
    if (!locationDoc.exists) {
      return res.status(404).json({ error: 'Location not found' });
    }
    
    const location = locationDoc.data();
    
    // Get weather data from OpenWeatherMap API
    const response = await axios.get(`${config.apis.weather.baseUrl}/weather`, {
      params: {
        lat: location.latitude,
        lon: location.longitude,
        units: 'metric',
        appid: config.apis.weather.apiKey
      },
      timeout: config.apis.weather.requestTimeoutMs
    });
    
    const weatherData = {
      location: {
        id: locationId,
        name: location.name,
        latitude: location.latitude,
        longitude: location.longitude
      },
      temperature: response.data.main.temp,
      humidity: response.data.main.humidity,
      windSpeed: response.data.wind.speed,
      windDirection: response.data.wind.deg,
      precipitation: response.data.rain ? response.data.rain['1h'] : 0,
      pressure: response.data.main.pressure,
      condition: response.data.weather[0].main,
      description: response.data.weather[0].description,
      timestamp: new Date().toISOString()
    };
    
    res.json(weatherData);
  } catch (error) {
    next(error);
  }
});

// Get historical weather data for a specific location
router.get('/historical/:locationId', async (req, res, next) => {
  try {
    const locationId = req.params.locationId;
    const { startDate, endDate } = req.query;
    
    // Validate date parameters
    if (!startDate || !endDate) {
      return res.status(400).json({ error: 'Start date and end date are required' });
    }
    
    // Query BigQuery for historical weather data
    const query = `
      SELECT
        location_name,
        timestamp,
        temperature,
        humidity,
        wind_speed,
        wind_direction,
        precipitation,
        pressure,
        weather_condition
      FROM
        \`${config.google.projectId}.${config.bigquery.dataset}.${config.bigquery.tables.weatherData}\`
      WHERE
        location_id = @locationId
        AND timestamp BETWEEN TIMESTAMP(@startDate) AND TIMESTAMP(@endDate)
      ORDER BY
        timestamp ASC
    `;
    
    const options = {
      query,
      params: {
        locationId: locationId,
        startDate: startDate,
        endDate: endDate
      }
    };
    
    const [rows] = await bigquery.query(options);
    
    res.json({ 
      location: locationId, 
      startDate, 
      endDate, 
      data: rows 
    });
  } catch (error) {
    next(error);
  }
});

// Get weather stats for a specific location (min, max, avg)
router.get('/stats/:locationId', async (req, res, next) => {
  try {
    const locationId = req.params.locationId;
    const { period, date } = req.query;
    
    if (!period || !date) {
      return res.status(400).json({ error: 'Period and date are required' });
    }
    
    let query;
    let intervalStart, intervalEnd;
    
    const queryDate = new Date(date);
    
    // Determine query based on period
    switch (period) {
      case 'day':
        intervalStart = new Date(queryDate);
        intervalEnd = new Date(queryDate);
        intervalEnd.setDate(intervalEnd.getDate() + 1);
        
        query = `
          SELECT
            MIN(temperature) as min_temp,
            MAX(temperature) as max_temp,
            AVG(temperature) as avg_temp,
            MIN(humidity) as min_humidity,
            MAX(humidity) as max_humidity,
            AVG(humidity) as avg_humidity,
            AVG(wind_speed) as avg_wind_speed,
            AVG(pressure) as avg_pressure
          FROM
            \`${config.google.projectId}.${config.bigquery.dataset}.${config.bigquery.tables.weatherData}\`
          WHERE
            location_id = @locationId
            AND timestamp BETWEEN TIMESTAMP(@intervalStart) AND TIMESTAMP(@intervalEnd)
        `;
        break;
        
      case 'week':
        intervalStart = new Date(queryDate);
        // Set to the beginning of the week (Sunday)
        intervalStart.setDate(intervalStart.getDate() - intervalStart.getDay());
        
        intervalEnd = new Date(intervalStart);
        intervalEnd.setDate(intervalEnd.getDate() + 7);
        
        query = `
          SELECT
            EXTRACT(DAYOFWEEK FROM timestamp) as day_of_week,
            MIN(temperature) as min_temp,
            MAX(temperature) as max_temp,
            AVG(temperature) as avg_temp,
            AVG(humidity) as avg_humidity,
            AVG(wind_speed) as avg_wind_speed
          FROM
            \`${config.google.projectId}.${config.bigquery.dataset}.${config.bigquery.tables.weatherData}\`
          WHERE
            location_id = @locationId
            AND timestamp BETWEEN TIMESTAMP(@intervalStart) AND TIMESTAMP(@intervalEnd)
          GROUP BY
            day_of_week
          ORDER BY
            day_of_week
        `;
        break;
        
      case 'month':
        intervalStart = new Date(queryDate.getFullYear(), queryDate.getMonth(), 1);
        intervalEnd = new Date(queryDate.getFullYear(), queryDate.getMonth() + 1, 0);
        
        query = `
          SELECT
            EXTRACT(DAY FROM timestamp) as day,
            MIN(temperature) as min_temp,
            MAX(temperature) as max_temp,
            AVG(temperature) as avg_temp,
            AVG(humidity) as avg_humidity
          FROM
            \`${config.google.projectId}.${config.bigquery.dataset}.${config.bigquery.tables.weatherData}\`
          WHERE
            location_id = @locationId
            AND timestamp BETWEEN TIMESTAMP(@intervalStart) AND TIMESTAMP(@intervalEnd)
          GROUP BY
            day
          ORDER BY
            day
        `;
        break;
        
      default:
        return res.status(400).json({ error: 'Invalid period. Use day, week, or month.' });
    }
    
    const options = {
      query,
      params: {
        locationId: locationId,
        intervalStart: intervalStart.toISOString(),
        intervalEnd: intervalEnd.toISOString()
      }
    };
    
    const [rows] = await bigquery.query(options);
    
    res.json({
      location: locationId,
      period: period,
      date: date,
      intervalStart: intervalStart.toISOString(),
      intervalEnd: intervalEnd.toISOString(),
      data: rows
    });
  } catch (error) {
    next(error);
  }
});

module.exports = router;