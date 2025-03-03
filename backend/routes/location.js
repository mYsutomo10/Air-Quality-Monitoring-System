const express = require('express');
const router = express.Router();
const admin = require('firebase-admin');
const db = admin.firestore();

// Get all available locations
router.get('/', async (req, res) => {
  try {
    const locationsSnapshot = await db.collection('locations').get();
    
    if (locationsSnapshot.empty) {
      return res.status(404).json({ error: 'No locations found' });
    }
    
    const locations = [];
    locationsSnapshot.forEach(doc => {
      locations.push({
        id: doc.id,
        ...doc.data()
      });
    });
    
    return res.status(200).json(locations);
  } catch (error) {
    console.error('Error getting locations:', error);
    return res.status(500).json({ error: 'Failed to retrieve locations' });
  }
});

// Search for locations by name or region
router.get('/search', async (req, res) => {
  try {
    const { query } = req.query;
    
    if (!query || query.length < 2) {
      return res.status(400).json({ error: 'Search query must be at least 2 characters' });
    }
    
    // Convert query to lowercase for case-insensitive search
    const searchQuery = query.toLowerCase();
    
    // Get all locations to perform search
    // Note: In a production environment with many locations, you'd want to use
    // a more efficient searching mechanism or a dedicated search service
    const locationsSnapshot = await db.collection('locations').get();
    
    const matchingLocations = [];
    locationsSnapshot.forEach(doc => {
      const locationData = doc.data();
      const name = locationData.name.toLowerCase();
      const region = locationData.region ? locationData.region.toLowerCase() : '';
      
      if (name.includes(searchQuery) || region.includes(searchQuery)) {
        matchingLocations.push({
          id: doc.id,
          ...locationData
        });
      }
    });
    
    return res.status(200).json(matchingLocations);
  } catch (error) {
    console.error('Error searching locations:', error);
    return res.status(500).json({ error: 'Failed to search locations' });
  }
});

// Get specific location by ID
router.get('/:locationId', async (req, res) => {
  try {
    const locationId = req.params.locationId;
    const locationDoc = await db.collection('locations').doc(locationId).get();
    
    if (!locationDoc.exists) {
      return res.status(404).json({ error: 'Location not found' });
    }
    
    return res.status(200).json({
      id: locationDoc.id,
      ...locationDoc.data()
    });
  } catch (error) {
    console.error('Error getting location:', error);
    return res.status(500).json({ error: 'Failed to retrieve location' });
  }
});

module.exports = router;