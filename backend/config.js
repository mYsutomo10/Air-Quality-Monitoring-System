// Configuration for the AQMS backend
require('dotenv').config(); // Load environment variables from .env file

const config = {
  // Google Cloud Project Configuration
  google: {
    projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
    keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
  },
  
  // BigQuery Configuration
  bigquery: {
    dataset: process.env.BIGQUERY_DATASET || 'aqms_dataset',
    tables: {
      historicalAqi: 'historical_aqi',
      weatherData: 'weather_data',
      satelliteData: 'satellite_data',
      userFeedback: 'user_feedback'
    }
  },
  
  // Firestore Configuration
  firestore: {
    collections: {
      currentAqi: 'current_aqi',
      userFeedback: 'user_feedback',
      locations: 'locations'
    }
  },
  
  // External API Configurations
  apis: {
    weather: {
      apiKey: process.env.WEATHER_API_KEY,
      baseUrl: process.env.WEATHER_API_URL || 'https://api.openweathermap.org/data/2.5',
      requestTimeoutMs: 5000
    },
    satellite: {
      apiKey: process.env.SENTINEL_API_KEY,
      baseUrl: process.env.SENTINEL_API_URL || 'https://api.sentinel-hub.com',
      requestTimeoutMs: 10000
    }
  },
  
  // Cache Configuration
  cache: {
    enabled: process.env.ENABLE_CACHE === 'true',
    ttlSeconds: parseInt(process.env.CACHE_TTL_SECONDS || '300', 10)
  },
  
  // Logging Configuration
  logging: {
    level: process.env.LOG_LEVEL || 'info'
  },
  
  // CORS Configuration
  cors: {
    allowedOrigins: process.env.CORS_ALLOWED_ORIGINS ? 
      process.env.CORS_ALLOWED_ORIGINS.split(',') : 
      ['http://localhost:8080']
  },
  
  // Rate Limiting Configuration
  rateLimit: {
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: parseInt(process.env.RATE_LIMIT_MAX || '100', 10)
  }
};

module.exports = config;