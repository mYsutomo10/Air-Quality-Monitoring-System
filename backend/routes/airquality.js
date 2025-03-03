const express = require('express');
const router = express.Router();
const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const admin = require('firebase-admin');
const config = require('../config');

const storage = new Storage();
const bigquery = new BigQuery();
const db = admin.firestore();

// Get current air quality data for a specific location
router.get('/current/:locationId', async (req, res) => {
  try {
    const locationId = req.params.locationId;
    
    // Get latest AQI data from Firestore
    const aqiDoc = await db.collection('airquality')
      .where('locationId', '==', locationId)
      .orderBy('timestamp', 'desc')
      .limit(1)
      .get();
    
    if (aqiDoc.empty) {
      return res.status(404).json({ error: 'Air quality data not found for this location' });
    }
    
    const aqiData = aqiDoc.docs[0].data();
    return res.status(200).json(aqiData);
  } catch (error) {
    console.error('Error getting air quality data:', error);
    return res.status(500).json({ error: 'Failed to retrieve air quality data' });
  }
});

// Get historical air quality data for a specific location
router.get('/historical/:locationId', async (req, res) => {
  try {
    const locationId = req.params.locationId;
    const { interval, startDate, endDate, pollutant } = req.query;
    
    let query = `
      SELECT timestamp, aqi, pm25, pm10, o3, co, no2
      FROM \`${config.bigquery.datasetId}.air_quality_data\`
      WHERE location_id = @locationId
    `;
    
    const queryParams = {
      locationId: locationId
    };
    
    if (startDate && endDate) {
      query += ' AND timestamp BETWEEN @startDate AND @endDate';
      queryParams.startDate = startDate;
      queryParams.endDate = endDate;
    }
    
    if (pollutant && ['pm25', 'pm10', 'o3', 'co', 'no2'].includes(pollutant)) {
      // If specific pollutant requested, we can optimize the query
      query = query.replace('aqi, pm25, pm10, o3, co, no2', `aqi, ${pollutant}`);
    }
    
    switch (interval) {
      case 'hourly':
        query += ' GROUP BY timestamp, aqi, pm25, pm10, o3, co, no2 ORDER BY timestamp';
        break;
      case 'daily':
        query = query.replace('SELECT timestamp,', 'SELECT DATE(timestamp) as date,');
        query += `
          GROUP BY date
          ORDER BY date
        `;
        break;
      case 'weekly':
        query = query.replace('SELECT timestamp,', 'SELECT EXTRACT(WEEK FROM timestamp) as week, EXTRACT(YEAR FROM timestamp) as year,');
        query += `
          GROUP BY week, year
          ORDER BY year, week
        `;
        break;
      case 'monthly':
        query = query.replace('SELECT timestamp,', 'SELECT EXTRACT(MONTH FROM timestamp) as month, EXTRACT(YEAR FROM timestamp) as year,');
        query += `
          GROUP BY month, year
          ORDER BY year, month
        `;
        break;
      default:
        // Default to raw data with reasonable limit
        query += ' ORDER BY timestamp DESC LIMIT 1000';
    }
    
    const options = {
      query: query,
      params: queryParams
    };
    
    const [rows] = await bigquery.query(options);
    return res.status(200).json(rows);
  } catch (error) {
    console.error('Error getting historical air quality data:', error);
    return res.status(500).json({ error: 'Failed to retrieve historical air quality data' });
  }
});

// Get pollutant levels for a specific location
router.get('/pollutants/:locationId', async (req, res) => {
  try {
    const locationId = req.params.locationId;
    
    // Get latest pollutant data from Firestore
    const pollutantDoc = await db.collection('airquality')
      .where('locationId', '==', locationId)
      .orderBy('timestamp', 'desc')
      .limit(1)
      .get();
    
    if (pollutantDoc.empty) {
      return res.status(404).json({ error: 'Pollutant data not found for this location' });
    }
    
    const aqiData = pollutantDoc.docs[0].data();
    
    // Extract just the pollutant data
    const pollutantData = {
      timestamp: aqiData.timestamp,
      pm25: aqiData.pm25,
      pm10: aqiData.pm10,
      o3: aqiData.o3,
      co: aqiData.co,
      no2: aqiData.no2,
      primaryPollutant: aqiData.primaryPollutant
    };
    
    return res.status(200).json(pollutantData);
  } catch (error) {
    console.error('Error getting pollutant data:', error);
    return res.status(500).json({ error: 'Failed to retrieve pollutant data' });
  }
});

// Get AQI map data for multiple locations
router.get('/map', async (req, res) => {
  try {
    // Get latest AQI data for all locations from Firestore
    const aqiSnapshot = await db.collection('airquality')
      .orderBy('timestamp', 'desc')
      .get();
    
    // Group by locationId, keeping only the latest entry for each location
    const locationMap = new Map();
    aqiSnapshot.forEach(doc => {
      const data = doc.data();
      if (!locationMap.has(data.locationId) || 
          data.timestamp > locationMap.get(data.locationId).timestamp) {
        locationMap.set(data.locationId, data);
      }
    });
    
    const mapData = Array.from(locationMap.values()).map(data => ({
      locationId: data.locationId,
      locationName: data.locationName,
      latitude: data.latitude,
      longitude: data.longitude,
      aqi: data.aqi,
      aqiCategory: data.aqiCategory,
      primaryPollutant: data.primaryPollutant,
      timestamp: data.timestamp
    }));
    
    return res.status(200).json(mapData);
  } catch (error) {
    console.error('Error getting map data:', error);
    return res.status(500).json({ error: 'Failed to retrieve map data' });
  }
});

module.exports = router;