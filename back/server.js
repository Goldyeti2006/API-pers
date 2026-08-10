import express from "express";
import { createClient } from 'redis';
import { Queue, Worker } from 'bullmq';

const redisClient = createClient();
const app = express();
const port = 3000;
const VALID_API_KEY = "key_123";
app.set('trust proxy', 1);
const bullmqConnection = {
    host: '127.0.0.1',
    port: 6379,
};
const requestQueue = new Queue('proxy-requests', { connection: bullmqConnection });
const worker = new Worker('proxy-requests', async job => {
    console.log(`Processing job ${job.id} with data:`, job.data);
}, { connection: bullmqConnection });

redisClient.on('error', (err) => console.error('Redis Client Error', err));
await redisClient.connect();
const rateLimiter = async (req, res, next) => {
    const userIp = req.ip;
    const userKey = req.headers['x-api-key'];
    const uniqueIdentifier = `${userIp}:${userKey}`;
    const requestCount = await redisClient.incr(uniqueIdentifier);
    console.log("Current count for this user:", requestCount);
    if (requestCount === 1) {
    await redisClient.expire(uniqueIdentifier, 60);
    }
    if (requestCount && parseInt(requestCount) >= 5) {
        res.status(429).json({ error: 'Too many requests' });
    }else {
        next();
    }
}
const checkApikey = (req, res, next) => {
    const api_key = req.headers['x-api-key'];
    if (api_key !== VALID_API_KEY) {
       res.status(401).json({ error: 'Unauthorized' });
    }
    next();
};

app.get('/api/character',checkApikey, rateLimiter, async (req, res) => {
    try {       
        const response = await requestQueue.add('https://rickandmortyapi.com/api/character/108');        
        const data = await response.json();
        res.json(data);
    } catch (error) {
        console.error('Error fetching character data:', error);
        res.status(500).json({ error: 'Failed to fetch character data' });
    }
});
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});