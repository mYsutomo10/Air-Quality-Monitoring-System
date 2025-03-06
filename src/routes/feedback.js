const express = require('express');
const router = express.Router();
const admin = require('firebase-admin');
const db = admin.firestore();

// Submit user feedback
router.post('/', async (req, res) => {
  try {
    const { userId, locationId, feedbackType, message, rating } = req.body;
    
    // Validate required fields
    if (!feedbackType || !message) {
      return res.status(400).json({ error: 'Feedback type and message are required' });
    }
    
    // Create feedback document
    const feedbackData = {
      userId: userId || 'anonymous',
      locationId: locationId || null,
      feedbackType,
      message,
      rating: rating || null,
      timestamp: admin.firestore.FieldValue.serverTimestamp(),
      status: 'pending'
    };
    
    const feedbackRef = await db.collection('feedback').add(feedbackData);
    
    return res.status(201).json({ 
      id: feedbackRef.id,
      message: 'Feedback submitted successfully'
    });
  } catch (error) {
    console.error('Error submitting feedback:', error);
    return res.status(500).json({ error: 'Failed to submit feedback' });
  }
});

// Get feedback for a specific location
router.get('/location/:locationId', async (req, res) => {
  try {
    const locationId = req.params.locationId;
    
    // Only return approved feedback
    const feedbackSnapshot = await db.collection('feedback')
      .where('locationId', '==', locationId)
      .where('status', '==', 'approved')
      .orderBy('timestamp', 'desc')
      .limit(20)
      .get();
    
    const feedback = [];
    feedbackSnapshot.forEach(doc => {
      feedback.push({
        id: doc.id,
        ...doc.data()
      });
    });
    
    return res.status(200).json(feedback);
  } catch (error) {
    console.error('Error getting feedback:', error);
    return res.status(500).json({ error: 'Failed to retrieve feedback' });
  }
});

module.exports = router;