require('dotenv').config();
const { MongoClient } = require('mongodb');

async function promote() {
  const client = new MongoClient(process.env.MONGODB_URI);
  try {
    await client.connect();
    const db = client.db('marketData');
    const result = await db.collection('users').updateOne(
      { email: 'riderapp80@gmail.com' },
      {
        $set: {
          'subscription.plan': 'Enterprise',
          'subscription.subscriptionId': 'manual-ent-2025',
          'subscription.startDate': new Date().toISOString(),
          'subscription.active': true
        }
      }
    );
    console.log('Success:', result.modifiedCount, 'user updated');
  } catch (err) {
    console.error('Error:', err);
  } finally {
    await client.close();
  }
}

promote();