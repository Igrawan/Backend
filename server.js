require('dotenv').config();
const express = require('express');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const { OAuth2Client } = require('google-auth-library');
const axios = require('axios');
const bodyParser = require('body-parser');
const { param, query, validationResult } = require('express-validator');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const winston = require('winston');

// Configuration
const GENAI_API_KEYS = [
  process.env.GENAI_API_KEY1,
  process.env.GENAI_API_KEY2,
  process.env.GENAI_API_KEY3,
  process.env.GENAI_API_KEY4,
  process.env.GENAI_API_KEY5,
  process.env.GENAI_API_KEY6,
  process.env.GENAI_API_KEY7,
  process.env.GENAI_API_KEY8,
  process.env.GENAI_API_KEY9,
  process.env.GENAI_API_KEY10,
  process.env.GENAI_API_KEY11,
  process.env.GENAI_API_KEY12,
  process.env.GENAI_API_KEY13,
  process.env.GENAI_API_KEY14,
  process.env.GENAI_API_KEY15,
  process.env.GENAI_API_KEY16,
  process.env.GENAI_API_KEY17,
  process.env.GENAI_API_KEY18,
  process.env.GENAI_API_KEY19,
  process.env.GENAI_API_KEY20,
  process.env.GENAI_API_KEY21,
  process.env.GENAI_API_KEY22
];
const BINANCE_WS_URL = process.env.BINANCE_WS_URL;
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour TTL
const DATA_WINDOW_MS = 24 * 60 * 60 * 1000; // 24 hours
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;
const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = 'marketData';
const JWT_SECRET = process.env.JWT_SECRET;
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const PAYPAL_CLIENT_ID = process.env.PAYPAL_CLIENT_ID;
const PAYPAL_SECRET = process.env.PAYPAL_SECRET;

const TIMEFRAMES = ['1h', '3h', '6h', '12h', '24h'];
const TIMEFRAME_MS = {
  '1h': 60 * 60 * 1000,
  '3h': 3 * 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '12h': 12 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000
};

const PLAN_CURRENCIES = {
  Free: ['BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'LTC/USDT', 'DOGE/USDT'], // Free tier: 5 currencies
  Basic: ['BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'LTC/USDT', 'BCH/USDT', 'ADA/USDT', 'DOT/USDT', 'LINK/USDT', 'BNB/USDT', 'DOGE/USDT'],
  Pro: ['BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'LTC/USDT', 'BCH/USDT', 'ADA/USDT', 'DOT/USDT', 'LINK/USDT', 'BNB/USDT', 'DOGE/USDT', 'SOL/USDT', 'MATIC/USDT', 'AVAX/USDT', 'ATOM/USDT', 'UNI/USDT'],
  Enterprise: 'all' // All currencies
};

const FREE_TIER_ACCESS = {
  timeframes: ['12h', '24h'],
  endpoints: ['news', 'sentiment', 'all-impact-news','rate']
};

const NODE_ENV = process.env.NODE_ENV || 'development';
const isProduction = NODE_ENV === 'production';

// Logger setup
const logger = winston.createLogger({
  level: isProduction ? 'warn' : 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.simple(),
    }),
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' })
  ]
});

// Google OAuth2 client
const googleClient = new OAuth2Client(GOOGLE_CLIENT_ID);

// MongoDB client (Singleton)
let mongoInstance = null;

class MongoDBClient {
  static getInstance() {
    if (!mongoInstance) {
      mongoInstance = new MongoDBClient();
      mongoInstance.connect();
    }
    return mongoInstance;
  }

  constructor() {
    if (mongoInstance) throw new Error('Use getInstance()');
    this.client = new MongoClient(MONGODB_URI, {
       maxPoolSize: 10,
       minPoolSize: 2,
       maxIdleTimeMS: 30000,
       serverSelectionTimeoutMS: 5000
      });
    this.db = this.client.db(DB_NAME);
    this.newsCollection = this.db.collection('newsCache');
    this.sentimentCollection = this.db.collection('sentimentCache');
    this.psychologyCollection = this.db.collection('psychologyCache');
    this.analysisCollection = this.db.collection('analysisCache');
    this.marketDataCollection = this.db.collection('marketDataCache');
    this.usersCollection = this.db.collection('users');
  }

  async connect() {
    try {
      await this.client.connect();
      logger.info('Connected to MongoDB');
      await this.createIndexes();
    } catch (error) {
      logger.error('Failed to connect to MongoDB:', { error: error.message });
      throw error;
    }
  }

  async createIndexes() {
    try {
      const collections = [
        { collection: this.newsCollection, name: 'newsCache' },
        { collection: this.sentimentCollection, name: 'sentimentCache' },
        { collection: this.psychologyCollection, name: 'psychologyCache' },
        { collection: this.analysisCollection, name: 'analysisCache' },
        { collection: this.marketDataCollection, name: 'marketDataCache' },
        { collection: this.usersCollection, name: 'users' }
      ];

      for (const { collection, name } of collections) {
        try {
          await collection.dropIndex('symbol_1');
          logger.info(`Dropped old symbol_1 index from ${name}`);
        } catch (error) {
          if (error.message.includes('index not found')) {
            logger.info(`No symbol_1 index to drop in ${name}`);
          } else {
            logger.error(`Error dropping symbol_1 index from ${name}:`, { error: error.message });
          }
        }
      }

      await Promise.all([
        this.newsCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.sentimentCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.psychologyCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.analysisCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.marketDataCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.usersCollection.createIndex({ email: 1 }, { unique: true })
      ]);

      logger.info('Created new indexes successfully');
    } catch (error) {
      logger.error('Error creating indexes:', { error: error.message });
      throw error;
    }
  }

  async getCache(collection, symbol, timeframe) {
    return await collection.findOne({ symbol, timeframe });
  }

  async setCache(collection, symbol, timeframe, data, timestamp) {
    await collection.updateOne(
      { symbol, timeframe },
      { $set: { symbol, timeframe, data, timestamp } },
      { upsert: true }
    );
  }

  async deleteCache(collection, symbol, timeframe) {
    await collection.deleteOne({ symbol, timeframe });
  }

  async getNewsCache(symbol, timeframe) {
    return await this.getCache(this.newsCollection, symbol, timeframe);
  }

  async setNewsCache(symbol, timeframe, data, timestamp) {
    await this.setCache(this.newsCollection, symbol, timeframe, data, timestamp);
  }

  async deleteNewsCache(symbol, timeframe) {
    await this.deleteCache(this.newsCollection, symbol, timeframe);
  }

  async getSentimentCache(symbol, timeframe) {
    return await this.getCache(this.sentimentCollection, symbol, timeframe);
  }

  async setSentimentCache(symbol, timeframe, data, timestamp) {
    await this.setCache(this.sentimentCollection, symbol, timeframe, data, timestamp);
  }

  async deleteSentimentCache(symbol, timeframe) {
    await this.deleteCache(this.sentimentCollection, symbol, timeframe);
  }

  async getPsychologyCache(symbol, timeframe) {
    return await this.getCache(this.psychologyCollection, symbol, timeframe);
  }

  async setPsychologyCache(symbol, timeframe, data, timestamp) {
    await this.setCache(this.psychologyCollection, symbol, timeframe, data, timestamp);
  }

  async deletePsychologyCache(symbol, timeframe) {
    await this.deleteCache(this.psychologyCollection, symbol, timeframe);
  }

  async getAnalysisCache(symbol, timeframe) {
    return await this.getCache(this.analysisCollection, symbol, timeframe);
  }

  async setAnalysisCache(symbol, timeframe, data, timestamp) {
    await this.setCache(this.analysisCollection, symbol, timeframe, data, timestamp);
  }

  async deleteAnalysisCache(symbol, timeframe) {
    await this.deleteCache(this.analysisCollection, symbol, timeframe);
  }

  async getMarketDataCache(symbol, timeframe) {
    return await this.getCache(this.marketDataCollection, symbol, timeframe);
  }

  async setMarketDataCache(symbol, timeframe, data, timestamp) {
    await this.setCache(this.marketDataCollection, symbol, timeframe, data, timestamp);
  }

  async deleteMarketDataCache(symbol, timeframe) {
    await this.deleteCache(this.marketDataCollection, symbol, timeframe);
  }

  async getAllCachedSymbols() {
    return await this.newsCollection.distinct('symbol');
  }

  async getAllImpactNews(timeframe) {
    const symbols = await this.getAllCachedSymbols();
    const allImpactNews = [];
    
    for (const symbol of symbols) {
      const cacheEntry = await this.getNewsCache(symbol, timeframe);
      if (cacheEntry?.data) {
        const newsItems = cacheEntry.data
          .filter(item => item.impact === 'high')
          .map(item => ({ symbol, news: item }));
        allImpactNews.push(...newsItems);
      }
    }

    return allImpactNews.sort((a, b) => {
      const timeDiff = new Date(b.news.timestamp).getTime() - new Date(a.news.timestamp).getTime();
      if (timeDiff !== 0) return timeDiff;
      const impactOrder = { high: 3, medium: 2, low: 1 };
      return impactOrder[b.news.impact] - impactOrder[a.news.impact];
    });
  }

  async getUserByEmail(email) {
    return await this.usersCollection.findOne({ email });
  }

  async createOrUpdateUser(user) {
    await this.usersCollection.updateOne(
      { email: user.email },
      { $set: user },
      { upsert: true }
    );
  }

  async cleanupOldData() {
    try {
      const collections = [
        this.newsCollection,
        this.sentimentCollection,
        this.psychologyCollection,
        this.analysisCollection,
        this.marketDataCollection
      ];

      await Promise.all(
        collections.map(collection => 
          collection.deleteMany({ timeframe: { $exists: false } })
            .then(() => logger.info(`Cleaned up old data from ${collection.collectionName}`))
        )
      );
    } catch (error) {
      logger.error('Error cleaning up old data:', { error: error.message });
    }
  }
}

// Google GenAI client
class GoogleGenAIClient {
  constructor() {
    if (GENAI_API_KEYS.length === 0) {
      throw new Error('No Gemini API keys provided');
    }
    this.models = GENAI_API_KEYS.map(key => new GoogleGenerativeAI(key).getGenerativeModel({ model: 'gemini-2.0-flash' }));
    this.currentIndex = 0;
  }

  getCurrentModel() {
    return this.models[this.currentIndex];
  }

  switchToNextModel() {
    this.currentIndex = (this.currentIndex + 1) % this.models.length;
    logger.info(`Switched to API key index: ${this.currentIndex}`);
  }

  async fetchRealTimeNews(symbol, timeframe) {
    try {
      const normalizedSymbol = symbol.split('/')[0].toUpperCase();
      const endTime = new Date();
      const startTime = new Date(endTime.getTime() - TIMEFRAME_MS[timeframe]);

      let lastError = null;
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        const model = this.getCurrentModel();
        try {
          const prompt = `Provide news analysis for ${normalizedSymbol} cryptocurrency from ${startTime.toISOString()} to ${endTime.toISOString()}. Include sentiment (bullish, bearish, neutral), impact level (high, medium, low), and brief summaries. Return in JSON format with fields: title, sentiment, impact, timestamp (ISO format), source, summary. Limit to 10 items per ${timeframe}, prioritizing high-impact news, sorted by recency (newest first) and then by impact (high to low). Respond ONLY with the JSON array. No additional text or explanations.`;

          const result = await model.generateContent(prompt);
          const response = await result.response;
          let jsonString = response.text().trim();

          if (jsonString.startsWith('```json') && jsonString.endsWith('```')) {
            jsonString = jsonString.slice(7, -3).trim();
          } else if (jsonString.startsWith('```') && jsonString.endsWith('```')) {
            jsonString = jsonString.slice(3, -3).trim();
          }

          let newsData;
          try {
            newsData = JSON.parse(jsonString);
            if (!Array.isArray(newsData)) {
              throw new Error('Response is not an array');
            }
          } catch (parseError) {
            logger.error(`JSON parsing failed for ${symbol} (${timeframe}):`, { error: parseError.message, raw: jsonString });
            throw new Error('Invalid JSON format in response');
          }

          const impactOrder = { high: 3, medium: 2, low: 1 };
          return newsData
            .filter(item => {
              const itemTime = new Date(item.timestamp);
              const isValid = item.title &&
                ['bullish', 'bearish', 'neutral'].includes(item.sentiment) &&
                ['high', 'medium', 'low'].includes(item.impact) &&
                itemTime >= startTime && itemTime <= endTime;
              if (!isValid) {
                logger.warn(`Invalid news item for ${symbol} (${timeframe}):`, { item });
              }
              return isValid;
            })
            .sort((a, b) => {
              const timeDiff = new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime();
              return timeDiff !== 0 ? timeDiff : impactOrder[b.impact] - impactOrder[a.impact];
            })
            .slice(0, 10);
        } catch (error) {
          lastError = error;
          if (error.message.includes('429') || error.message.includes('rate limit') || (error.response && error.response.status === 429)) {
            logger.warn(`Rate limit hit on key ${this.currentIndex}, switching to next key`);
            this.switchToNextModel();
          }
          if (attempt < MAX_RETRIES) {
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
          }
        }
      }
      throw lastError || new Error('Failed to fetch news after retries');
    } catch (error) {
      logger.error(`Error fetching news for ${symbol} (${timeframe}):`, { error: error.message });
      return [{
        title: `No recent news for ${symbol}`,
        sentiment: 'neutral',
        impact: 'low',
        timestamp: new Date().toISOString(),
        source: 'System',
        summary: 'No news available for the requested timeframe.'
      }];
    }
  }

  async fetchMarketAnalysis(symbol, timeframe, news, sentiment, market, psychology) {
    try {
      const normalizedSymbol = symbol.split('/')[0].toUpperCase();
      const endTime = new Date();
      const startTime = new Date(endTime.getTime() - TIMEFRAME_MS[timeframe]);

      let lastError = null;
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        const model = this.getCurrentModel();
        try {
          const prompt = `
            Provide a comprehensive market analysis for ${normalizedSymbol} cryptocurrency over the ${timeframe} timeframe from ${startTime.toISOString()} to ${endTime.toISOString()}. Use the following data:
            - News: ${JSON.stringify(news)}
            - Sentiment: ${JSON.stringify(sentiment)}
            - Market Data: ${market ? JSON.stringify(market) : 'No market data available'}
            - Psychology: ${JSON.stringify(psychology)}
            Return the analysis in JSON format with the following fields:
            - marketImpact: A string summarizing the overall market impact based on news and market data.
            - tradingOpportunities: A string describing potential trading strategies or opportunities, integrating sentiment and psychology metrics.
            - keyRisks: A string outlining key risks to consider for trading or market participation, considering sentiment and psychology.
            - sentimentAnalysis: A string explicitly analyzing the market sentiment (bullish, bearish, neutral percentages) and its implications.
            - psychologyAnalysis: A string explicitly analyzing the market psychology (fear, greed, momentum, volatility) and its implications.
            Ensure the analysis is concise, actionable, and directly incorporates the provided sentiment and psychology data alongside news and market data. Avoid speculative or unsupported claims. Respond ONLY with valid JSON. No additional text or explanations.
          `;

          const result = await model.generateContent(prompt);
          const response = await result.response;
          let jsonString = response.text().trim();

          if (jsonString.startsWith('```json') && jsonString.endsWith('```')) {
            jsonString = jsonString.slice(7, -3).trim();
          } else if (jsonString.startsWith('```') && jsonString.endsWith('```')) {
            jsonString = jsonString.slice(3, -3).trim();
          }

          let analysis;
          try {
            analysis = JSON.parse(jsonString);
            if (
              !analysis.marketImpact ||
              !analysis.tradingOpportunities ||
              !analysis.keyRisks ||
              !analysis.sentimentAnalysis ||
              !analysis.psychologyAnalysis
            ) {
              throw new Error('Incomplete analysis data');
            }
          } catch (parseError) {
            logger.error(`JSON parsing failed for ${symbol} (${timeframe}) analysis:`, { error: parseError.message, raw: jsonString });
            throw new Error('Invalid JSON format in analysis response');
          }

          return analysis;
        } catch (error) {
          lastError = error;
          if (error.message.includes('429') || error.message.includes('rate limit') || (error.response && error.response.status === 429)) {
            logger.warn(`Rate limit hit on key ${this.currentIndex}, switching to next key`);
            this.switchToNextModel();
          }
          if (attempt < MAX_RETRIES) {
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
          }
        }
      }
      throw lastError || new Error('Failed to fetch market analysis after retries');
    } catch (error) {
      logger.error(`Error fetching market analysis for ${symbol} (${timeframe}):`, { error: error.message });
      return {
        marketImpact: `Unable to analyze market impact for ${symbol} due to data retrieval issues.`,
        tradingOpportunities: `No trading opportunities available due to insufficient data.`,
        keyRisks: `Risk assessment unavailable due to data retrieval issues.`,
        sentimentAnalysis: `Sentiment analysis unavailable due to data retrieval issues.`,
        psychologyAnalysis: `Psychology analysis unavailable due to data retrieval issues.`
      };
    }
  }
}

// Binance WebSocket client
class BinanceWSClient {
  constructor() {
    this.ws = null;
    this.marketDataHistory = new Map();
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.isConnecting = false;
    this.symbols = [
      'btcusdt', 'ethusdt', 'bnbusdt', 'xrpusdt', 'solusdt',
      'adausdt', 'dogeusdt', 'trxusdt', 'linkusdt', 'maticusdt',
      'dotusdt', 'ltcusdt', 'bchusdt', 'avaxusdt', 'xlmusdt',
      'uniusdt', 'atomusdt', 'etcusdt', 'filusdt', 'aptusdt',
      'arbusdt', 'opusdt', 'nearusdt', 'injusdt', 'tiausdt'
    ]; // 25 cryptocurrencies
    this.setupWebSocket();
  }

  async backfill(symbol) {
    const interval = '1m';
    let startTime = Date.now() - DATA_WINDOW_MS;
    const endTime = Date.now();
    const history = [];
    while (startTime < endTime) {
      try {
        const res = await axios.get('https://api.binance.com/api/v3/klines', {
          params: {
            symbol: symbol.toUpperCase(),
            interval,
            startTime,
            limit: 1000
          }
        });
        const klines = res.data;
        if (klines.length === 0) break;
        history.push(...klines.map(k => ({
          open: parseFloat(k[1]),
          high: parseFloat(k[2]),
          low: parseFloat(k[3]),
          close: parseFloat(k[4]),
          volume: parseFloat(k[5]),
          timestamp: k[6]
        })));
        startTime = klines[klines.length - 1][6] + 1;
      } catch (e) {
        logger.error(`Failed to backfill ${symbol}:`, { error: e.message });
        break;
      }
    }
    const sym = `${symbol.replace('usdt', '').toUpperCase()}/USDT`;
    this.marketDataHistory.set(sym, history.sort((a, b) => a.timestamp - b.timestamp));
  }

  async waitForWebSocketOpen() {
    const maxWaitTime = 10000; // 10 seconds max wait
    const checkInterval = 100; // Check every 100ms
    let elapsed = 0;

    while (this.ws.readyState !== WebSocket.OPEN && elapsed < maxWaitTime) {
      await new Promise(resolve => setTimeout(resolve, checkInterval));
      elapsed += checkInterval;
    }

    if (this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket failed to open within timeout');
    }
  }

  setupWebSocket() {
    if (this.isConnecting) return;
    this.isConnecting = true;

    this.ws = new WebSocket(BINANCE_WS_URL);

    this.ws.on('open', async () => {
      this.reconnectAttempts = 0;
      this.isConnecting = false;
      logger.info('WebSocket connected');

      try {
        // Backfill historical data for all symbols
        for (const sym of this.symbols) {
          await this.backfill(sym);
        }

        // Ensure WebSocket is open before sending subscriptions
        await this.waitForWebSocketOpen();

        // Subscribe to kline streams for all symbols
        this.symbols.forEach(sym => {
          try {
            this.ws.send(JSON.stringify({
              method: 'SUBSCRIBE',
              params: [`${sym}@kline_1m`],
              id: Date.now()
            }));
            logger.info(`Subscribed to ${sym}@kline_1m`);
          } catch (error) {
            logger.error(`Failed to subscribe to ${sym}:`, { error: error.message });
          }
        });
      } catch (error) {
        logger.error('Error during WebSocket subscription:', { error: error.message });
        this.handleReconnect();
      }
    });

    this.ws.on('message', (data) => {
      try {
        const message = JSON.parse(data.toString());
        if (message.e === 'kline') {
          const kline = message.k;
          if (kline.x) { // Only process closed candles
            const symbol = `${message.s.split('USDT')[0]}/USDT`;
            const marketData = {
              open: parseFloat(kline.o),
              high: parseFloat(kline.h),
              low: parseFloat(kline.l),
              close: parseFloat(kline.c),
              volume: parseFloat(kline.v),
              timestamp: kline.T
            };
            if (!this.marketDataHistory.has(symbol)) {
              this.marketDataHistory.set(symbol, []);
            }
            const history = this.marketDataHistory.get(symbol);
            if (!history.some(h => h.timestamp === marketData.timestamp)) {
              history.push(marketData);
            }
            const cutoff = Date.now() - DATA_WINDOW_MS;
            this.marketDataHistory.set(symbol, history.filter(d => d.timestamp >= cutoff));
          }
        }
      } catch (error) {
        logger.error('Error processing WebSocket message:', { error: error.message });
      }
    });

    this.ws.on('error', (error) => {
      logger.error('WebSocket error:', { error: error.message });
      this.isConnecting = false;
      this.handleReconnect();
    });

    this.ws.on('close', () => {
      logger.info('WebSocket closed');
      this.isConnecting = false;
      this.handleReconnect();
    });
  }

  handleReconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      const delay = RETRY_DELAY_MS * this.reconnectAttempts; // Linear delay
      logger.info(`Attempting to reconnect (${this.reconnectAttempts}/${this.maxReconnectAttempts}) in ${delay}ms`);
      setTimeout(() => {
        this.setupWebSocket();
      }, delay);
    } else {
      logger.error('Max reconnect attempts reached. WebSocket connection failed.');
    }
  }

  getMarketData(symbol, timeframe) {
    const history = this.marketDataHistory.get(symbol);
    if (!history || history.length === 0) return undefined;

    const cutoff = Date.now() - TIMEFRAME_MS[timeframe];
    let relevantData = history.filter(data => data.timestamp >= cutoff);

    if (relevantData.length === 0) return undefined;

    relevantData = relevantData.sort((a, b) => a.timestamp - b.timestamp);

    const open = relevantData[0].open;
    const high = Math.max(...relevantData.map(data => data.high));
    const low = Math.min(...relevantData.map(data => data.low));
    const close = relevantData[relevantData.length - 1].close;
    const volume = relevantData.reduce((sum, data) => sum + data.volume, 0);
    const change = open !== 0 ? ((close - open) / open) * 100 : 0;

    return {
      symbol,
      open: Number(open.toFixed(2)),
      high: Number(high.toFixed(2)),
      low: Number(low.toFixed(2)),
      close: Number(close.toFixed(2)),
      change: Number(change.toFixed(2)),
      volume: Number(volume.toFixed(2)),
      category: 'crypto',
      timestamp: Date.now()
    };
  }

  getCurrencyRate(symbol, timeframe) {
    const marketData = this.getMarketData(symbol, timeframe);
    if (!marketData) return undefined;

    return {
      symbol,
      rate: Number(marketData.close.toFixed(2)),
      timeframe,
      timestamp: Date.now()
    };
  }
}

// Market Data Service
class MarketDataService {
  constructor() {
    this.genAIClient = new GoogleGenAIClient();
    this.binanceClient = new BinanceWSClient();
    this.mongoClient = MongoDBClient.getInstance();
    this.initialize();
  }

  async initialize() {
    await this.mongoClient.cleanupOldData();
    this.scheduleHourlyUpdates();
  }

  scheduleHourlyUpdates() {
    const now = new Date();
    const msUntilNextHour = (60 - now.getMinutes()) * 60 * 1000 - now.getSeconds() * 1000 - now.getMilliseconds();

    setTimeout(() => {
      this.updateCaches();
      setInterval(() => this.updateCaches(), CACHE_TTL_MS);
    }, msUntilNextHour);
  }

  async updateCaches() {
    logger.info('Updating caches for hourly refresh');
    const now = new Date();
    const symbols = await this.mongoClient.getAllCachedSymbols();

    for (const symbol of symbols) {
      for (const timeframe of TIMEFRAMES) {
        try {
          const startTime = new Date(now.getTime() - TIMEFRAME_MS[timeframe]);
          const newNews = await this.genAIClient.fetchRealTimeNews(symbol, timeframe);
          
          const cacheEntry = await this.mongoClient.getNewsCache(symbol, timeframe);
          let allNews = cacheEntry?.data || [];
          // Dedupe by title and timestamp
          const existingKeys = new Set(allNews.map(n => `${n.title}|${n.timestamp}`));
          const filteredNew = newNews.filter(n => !existingKeys.has(`${n.title}|${n.timestamp}`));
          allNews = [...allNews, ...filteredNew].filter(item => {
            const itemTime = new Date(item.timestamp);
            return itemTime >= startTime && itemTime <= now;
          });

          await Promise.all([
            this.mongoClient.setNewsCache(symbol, timeframe, allNews, Date.now()),
            this.mongoClient.deleteSentimentCache(symbol, timeframe),
            this.mongoClient.deletePsychologyCache(symbol, timeframe),
            this.mongoClient.deleteAnalysisCache(symbol, timeframe),
            this.mongoClient.deleteMarketDataCache(symbol, timeframe)
          ]);
        } catch (error) {
          logger.error(`Error updating cache for ${symbol} (${timeframe}):`, { error: error.message });
        }
      }
    }
  }

  isCacheValid(cacheEntry) {
    if (!cacheEntry) return false;
    const age = Date.now() - cacheEntry.timestamp;
    return age <= CACHE_TTL_MS;
  }

  async getAllMarkets(timeframe) {
    const symbols = Array.from(this.binanceClient.marketDataHistory.keys());
    const markets = [];
    for (const symbol of symbols) {
      const marketData = await this.getMarketBySymbol(symbol, timeframe);
      if (marketData) markets.push(marketData);
    }
    return markets;
  }

  async getMarketBySymbol(symbol, timeframe) {
    const cacheEntry = await this.mongoClient.getMarketDataCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      logger.info(`Cache hit for market data: ${symbol} (${timeframe})`);
      return cacheEntry.data;
    }

    const marketData = this.binanceClient.getMarketData(symbol, timeframe);
    if (marketData) {
      await this.mongoClient.setMarketDataCache(symbol, timeframe, marketData, Date.now());
    }
    return marketData;
  }

  async getCurrencyRate(symbol, timeframe) {
    const cacheEntry = await this.mongoClient.getMarketDataCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      logger.info(`Cache hit for currency rate: ${symbol} (${timeframe})`);
      return {
        symbol,
        rate: Number(Number(cacheEntry.data.close).toFixed(2)),
        timeframe,
        timestamp: cacheEntry.data.timestamp
      };
    }

    const rateData = this.binanceClient.getCurrencyRate(symbol, timeframe);
    if (rateData) {
      await this.mongoClient.setMarketDataCache(symbol, timeframe, rateData, Date.now());
    }
    return rateData;
  }

  async getAllImpactNews(timeframe) {
    const currentTime = Date.now();
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const todayStartTime = todayStart.getTime();
  
    const symbols = await this.mongoClient.getAllCachedSymbols();
    let latestTimestamp = 0;
    for (const symbol of symbols) {
      const cacheEntry = await this.mongoClient.getNewsCache(symbol, timeframe);
      if (cacheEntry && cacheEntry.timestamp > latestTimestamp) {
        latestTimestamp = cacheEntry.timestamp;
      }
    }
    const cacheNeedsUpdate = latestTimestamp === 0 || (currentTime - latestTimestamp) > 10 * 60 * 1000;
  
    if (cacheNeedsUpdate) {
      logger.info(`Updating high-impact news cache for timeframe: ${timeframe}`);
      for (const symbol of symbols) {
        try {
          const news = await this.genAIClient.fetchRealTimeNews(symbol, timeframe);
  
          await this.mongoClient.setNewsCache(symbol, timeframe, news, currentTime);
        } catch (error) {
          logger.error(`Error updating news cache for ${symbol} (${timeframe}):`, { error: error.message });
        }
      }
    }
  
    const allImpactNews = [];
  
    for (const symbol of symbols) {
      const cacheEntry = await this.mongoClient.getNewsCache(symbol, timeframe);
      if (cacheEntry?.data) {
        const newsItems = cacheEntry.data
          .filter(item => {
            const itemTime = new Date(item.timestamp).getTime();
            return item.impact === 'high' && itemTime >= todayStartTime && itemTime <= currentTime;
          })
          .map(item => ({ symbol, news: item }));
        allImpactNews.push(...newsItems);
      }
    }
  
    return allImpactNews.sort((a, b) => new Date(b.news.timestamp).getTime() - new Date(a.news.timestamp).getTime());
  }

  async getTradingStrategies(symbol, timeframe) {
    const [market, news, sentiment] = await Promise.all([
      this.getMarketBySymbol(symbol, timeframe),
      this.getMarketNews(symbol, timeframe),
      this.getMarketSentiment(symbol, timeframe)
    ]);

    if (!market) return [];

    const priceChange = market.change;
    const volume = market.volume;
    const isBullish = sentiment.bullish > sentiment.bearish;
    const newsImpact = news.length > 0 ? news[0].impact : 'medium';

    const baseConfidence = newsImpact === 'high' ? 90 : newsImpact === 'medium' ? 80 : 70;
    const volumeIndicator = volume > 1000000 ? 'high' : volume > 500000 ? 'moderate' : 'low';
    const volatilityFactor = Math.abs(priceChange) > 5 ? 'high' : Math.abs(priceChange) > 2 ? 'moderate' : 'low';

    const timeframeStrategies = {
      '1h': { name: 'Hourly Surge', type: 'aggressive', timeframe: '5-15 minutes' },
      '3h': { name: 'Quarter-Day Momentum', type: 'balanced', timeframe: '2-6 hours' },
      '6h': { name: 'Half-Day Trend', type: 'balanced', timeframe: '2-6 hours' },
      '12h': { name: 'Intraday Trend', type: 'conservative', timeframe: '6-12 hours' },
      '24h': { name: 'Daily Trend', type: 'conservative', timeframe: '6-12 hours' }
    };

    const strategy = timeframeStrategies[timeframe];

    return [
      {
        name: strategy.name,
        type: strategy.type,
        description: isBullish
          ? `The market shows ${timeframe} bullish pressure with ${sentiment.bullish.toFixed(2)}% bullish sentiment and a price change of ${priceChange.toFixed(2)}%. High trading volume (${volumeIndicator}) and recent news (impact: ${newsImpact}) suggest opportunities for ${strategy.type} trading. Volatility is ${volatilityFactor}.`
          : `The market exhibits ${timeframe} bearish pressure with ${sentiment.bearish.toFixed(2)}% bearish sentiment and a price change of ${priceChange.toFixed(2)}%. Recent news (impact: ${newsImpact}) and ${volumeIndicator} volume create opportunities for ${strategy.type} trading. Volatility is ${volatilityFactor}.`,
        confidence: Math.min(95, baseConfidence + (volumeIndicator === 'high' ? 5 : 0)),
        timeframe: strategy.timeframe,
        advice: isBullish
          ? `Enter long positions on dips within ${timeframe}, targeting quick profits. Use tight stop-losses due to ${volatilityFactor} volatility. Monitor high-impact news.`
          : `Enter short positions or wait for bounces within ${timeframe}. Set stop-losses to manage ${volatilityFactor} volatility. Watch for news-driven reversals.`
      }
    ];
  }

  async getMarketNews(symbol, timeframe) {
    const cacheEntry = await this.mongoClient.getNewsCache(symbol, timeframe);
    const now = new Date();
    const cutoffTime = new Date(now.getTime() - TIMEFRAME_MS[timeframe]);

    if (this.isCacheValid(cacheEntry)) {
      logger.info(`Cache hit for news: ${symbol} (${timeframe})`);
      return cacheEntry.data.filter(item => new Date(item.timestamp) >= cutoffTime);
    }

    logger.info(`Cache miss for news: ${symbol} (${timeframe}), fetching from API`);
    const news = await this.genAIClient.fetchRealTimeNews(symbol, timeframe);
    logger.info(`Fetched ${news.length} news items for ${symbol} (${timeframe})`);
    await this.mongoClient.setNewsCache(symbol, timeframe, news, Date.now());
    return news;
  }

  async getMarketSentiment(symbol, timeframe) {
    const cacheEntry = await this.mongoClient.getSentimentCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      return cacheEntry.data;
    }

    const [news, market] = await Promise.all([
      this.getMarketNews(symbol, timeframe),
      this.getMarketBySymbol(symbol, timeframe)
    ]);

    const sentimentCount = news.reduce((acc, item) => {
      const impactWeight = { high: 3, medium: 2, low: 1 }[item.impact];
      acc[item.sentiment] += impactWeight;
      return acc;
    }, { bullish: 0, bearish: 0, neutral: 0 });

    const totalWeight = sentimentCount.bullish + sentimentCount.bearish + sentimentCount.neutral;
    
    let bullish = totalWeight > 0 ? (sentimentCount.bullish / totalWeight) * 100 : 33.33;
    let bearish = totalWeight > 0 ? (sentimentCount.bearish / totalWeight) * 100 : 33.33;
    let neutral = totalWeight > 0 ? (sentimentCount.neutral / totalWeight) * 100 : 33.34;

    if (market) {
      const priceChange = market.change;
      const volume = market.volume;

      const priceImpact = Math.min(Math.abs(priceChange) / 10, 0.15);
      if (priceChange > 0) {
        bullish += priceImpact * 30;
        bearish = Math.max(0, bearish - priceImpact * 15);
        neutral = Math.max(0, neutral - priceImpact * 15);
      } else if (priceChange < 0) {
        bearish += priceImpact * 30;
        bullish = Math.max(0, bullish - priceImpact * 15);
        neutral = Math.max(0, neutral - priceImpact * 15);
      }

      const volumeFactor = volume > 1000000 ? 0.15 : volume > 500000 ? 0.08 : 0.03;
      if (bullish > bearish) {
        bullish += volumeFactor * 20;
        bearish = Math.max(0, bearish - volumeFactor * 10);
        neutral = Math.max(0, neutral - volumeFactor * 10);
      } else if (bearish > bullish) {
        bearish += volumeFactor * 20;
        bullish = Math.max(0, bullish - volumeFactor * 10);
        neutral = Math.max(0, neutral - volumeFactor * 10);
      }

      const total = bullish + bearish + neutral;
      if (total > 0) {
        bullish = (bullish / total) * 100;
        bearish = (bearish / total) * 100;
        neutral = (neutral / total) * 100;
      } else {
        bullish = 33.33;
        bearish = 33.33;
        neutral = 33.34;
      }

      const sum = bullish + bearish + neutral;
      if (Math.abs(sum - 100) > 0.01) {
        const adjustment = 100 - sum;
        neutral += adjustment;
      }
    }

    const sentiment = {
      bullish: Number(Number(bullish).toFixed(2)),
      bearish: Number(Number(bearish).toFixed(2)),
      neutral: Number(Number(neutral).toFixed(2))
    };

    const finalSum = sentiment.bullish + sentiment.bearish + sentiment.neutral;
    if (Math.abs(finalSum - 100) > 0.01) {
      const factor = 100 / finalSum;
      sentiment.bullish = Number(Number(sentiment.bullish * factor).toFixed(2));
      sentiment.bearish = Number(Number(sentiment.bearish * factor).toFixed(2));
      sentiment.neutral = Number(Number(sentiment.neutral * factor).toFixed(2));
    }

    const adjustedSum = sentiment.bullish + sentiment.bearish + sentiment.neutral;
    if (Math.abs(adjustedSum - 100) > 0.01) {
      sentiment.neutral = Number((100 - sentiment.bullish - sentiment.bearish).toFixed(2));
    }

    await this.mongoClient.setSentimentCache(symbol, timeframe, sentiment, Date.now());
    return sentiment;
  }

  async getMarketPsychology(symbol, timeframe) {
    const cacheEntry = await this.mongoClient.getPsychologyCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      logger.info(`Cache hit for psychology: ${symbol} (${timeframe})`);
      return cacheEntry.data;
    }

    const [market, news, sentiment] = await Promise.all([
      this.getMarketBySymbol(symbol, timeframe),
      this.getMarketNews(symbol, timeframe),
      this.getMarketSentiment(symbol, timeframe)
    ]);

    if (!market) {
      return { fear: 33.33, greed: 33.33, momentum: 0, volatility: 0 };
    }

    const priceChange = market.change;
    const volume = market.volume;

    const newsImpactScore = news.reduce((acc, item) => {
      const impactValue = { high: 3, medium: 2, low: 1 }[item.impact];
      const sentimentMultiplier = item.sentiment === 'bullish' ? 1 : item.sentiment === 'bearish' ? -1 : 0;
      return acc + (impactValue * sentimentMultiplier);
    }, 0);

    const avgImpact = news.length > 0 
      ? news.reduce((acc, item) => acc + ({ high: 3, medium: 2, low: 1 }[item.impact]), 0) / news.length 
      : 1;

    let fear = 0;
    let greed = 0;
    let momentum = 0;
    let volatility = 0;

    if (priceChange < 0) {
      fear += Math.abs(priceChange) * 3;
    }
    fear += sentiment.bearish * 0.4;
    if (newsImpactScore < 0) {
      fear += Math.abs(newsImpactScore) * 1.5;
    }

    if (priceChange > 0) {
      greed += priceChange * 3;
    }
    greed += sentiment.bullish * 0.4;
    if (newsImpactScore > 0) {
      greed += newsImpactScore * 1.5;
    }

    momentum = Math.min(100, (
      (volume / 1500000) * 15 +
      Math.max(sentiment.bullish, sentiment.bearish) * 0.3 +
      avgImpact * 8
    ));

    volatility = Math.min(100, (
      Math.abs(priceChange) * 4 +
      (avgImpact - 1) * 15 +
      (news.length > 3 ? 15 : news.length * 3)
    ));

    const fearGreedTotal = fear + greed;
    if (fearGreedTotal > 0) {
      fear = (fear / fearGreedTotal) * 100;
      greed = (greed / fearGreedTotal) * 100;
    } else {
      fear = 50;
      greed = 50;
    }

    const psychology = {
      fear: Math.max(0, Math.min(100, Number(fear.toFixed(2)))),
      greed: Math.max(0, Math.min(100, Number(greed.toFixed(2)))),
      momentum: Math.max(0, Math.min(100, Number(momentum.toFixed(2)))),
      volatility: Math.max(0, Math.min(100, Number(volatility.toFixed(2))))
    };

    const fearGreedSum = psychology.fear + psychology.greed;
    if (Math.abs(fearGreedSum - 100) > 0.01) {
      logger.warn(`Fear/Greed sum for ${symbol} (${timeframe}) is ${fearGreedSum}. Normalizing...`);
      const factor = 100 / fearGreedSum;
      psychology.fear *= factor;
      psychology.greed *= factor;
    }

    await this.mongoClient.setPsychologyCache(symbol, timeframe, psychology, Date.now());
    logger.info(`Cached psychology for ${symbol} (${timeframe}):`, { psychology });
    return psychology;
  }

  async getMarketAnalysis(symbol, timeframe) {
    const cacheEntry = await this.mongoClient.getAnalysisCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      logger.info(`Cache hit for analysis: ${symbol} (${timeframe})`);
      return cacheEntry.data;
    }

    logger.info(`Cache miss for analysis: ${symbol} (${timeframe}), fetching from API`);
    const [news, sentiment, market, psychology] = await Promise.all([
      this.getMarketNews(symbol, timeframe),
      this.getMarketSentiment(symbol, timeframe),
      this.getMarketBySymbol(symbol, timeframe),
      this.getMarketPsychology(symbol, timeframe)
    ]);
    const analysis = await this.genAIClient.fetchMarketAnalysis(symbol, timeframe, news, sentiment, market, psychology);
    logger.info(`Fetched analysis for ${symbol} (${timeframe}):`, { analysis });
    await this.mongoClient.setAnalysisCache(symbol, timeframe, analysis, Date.now());
    return analysis;
  }
}

// Market Controller
class MarketController {
  static service = new MarketDataService();

  static getTimeframe(req) {
    const timeframe = req.query.timeframe;
    return TIMEFRAMES.includes(timeframe) ? timeframe : '24h';
  }

  static getSymbol(req) {
    const base = req.params.base?.toUpperCase() || '';
    const quote = req.params.quote?.toUpperCase() || '';
    return `${base}/${quote}`;
  }

  static async getAllMarkets(req, res) {
    try {
      const timeframe = MarketController.getTimeframe(req);
      const markets = await MarketController.service.getAllMarkets(timeframe);
      res.json(markets);
    } catch (error) {
      logger.error('Error fetching all markets:', { error: error.message });
      res.status(500).json({ error: 'Failed to fetch markets' });
    }
  }

  static async getMarketBySymbol(req, res) {
    try {
      const symbol = MarketController.getSymbol(req);
      const timeframe = MarketController.getTimeframe(req);
      logger.info(`Fetching market data for symbol: ${symbol}, timeframe: ${timeframe}`);
      const market = await MarketController.service.getMarketBySymbol(symbol, timeframe);
      return market ? res.json(market) : res.status(404).json({ error: 'Market not found' });
    } catch (error) {
      logger.error(`Error fetching market data for ${req.params.base}/${req.params.quote}:`, { error: error.message });
      res.status(500).json({ error: 'Failed to fetch market data' });
    }
  }

  static async getCurrencyRate(req, res) {
    try {
      const symbol = MarketController.getSymbol(req);
      const timeframe = MarketController.getTimeframe(req);
      logger.info(`Fetching currency rate for symbol: ${symbol}, timeframe: ${timeframe}`);
      const rateData = await MarketController.service.getCurrencyRate(symbol, timeframe);
      return rateData ? res.json(rateData) : res.status(404).json({ error: 'Currency rate not found' });
    } catch (error) {
      logger.error(`Error fetching currency rate for ${req.params.base}/${req.params.quote}:`, { error: error.message });
      res.status(500).json({ error: 'Failed to fetch currency rate' });
    }
  }

  static async getTradingStrategies(req, res) {
    try {
      const symbol = MarketController.getSymbol(req);
      const timeframe = MarketController.getTimeframe(req);
      logger.info(`Fetching trading strategies for symbol: ${symbol}, timeframe: ${timeframe}`);
      const strategies = await MarketController.service.getTradingStrategies(symbol, timeframe);
      res.json(strategies);
    } catch (error) {
      logger.error(`Error fetching trading strategies for ${req.params.base}/${req.params.quote}:`, { error: error.message });
      res.status(500).json({ error: 'Failed to fetch trading strategies' });
    }
  }

  static async getMarketNews(req, res) {
    try {
      const symbol = MarketController.getSymbol(req);
      const timeframe = MarketController.getTimeframe(req);
      logger.info(`Fetching news for symbol: ${symbol}, timeframe: ${timeframe}`);
      const news = await MarketController.service.getMarketNews(symbol, timeframe);
      res.json(news);
    } catch (error) {
      logger.error(`Error fetching news for ${req.params.base}/${req.params.quote}:`, { error: error.message });
      res.status(500).json({ error: 'Failed to fetch market news' });
    }
  }

  static async getMarketSentiment(req, res) {
    try {
      const symbol = MarketController.getSymbol(req);
      const timeframe = MarketController.getTimeframe(req);
      logger.info(`Fetching sentiment for symbol: ${symbol}, timeframe: ${timeframe}`);
      const sentiment = await MarketController.service.getMarketSentiment(symbol, timeframe);
      res.json(sentiment);
    } catch (error) {
      logger.error(`Error fetching sentiment for ${req.params.base}/${req.params.quote}:`, { error: error.message });
      res.status(500).json({ error: 'Failed to fetch market sentiment' });
    }
  }

  static async getMarketPsychology(req, res) {
    try {
      const symbol = MarketController.getSymbol(req);
      const timeframe = MarketController.getTimeframe(req);
      logger.info(`Fetching psychology for symbol: ${symbol}, timeframe: ${timeframe}`);
      const psychology = await MarketController.service.getMarketPsychology(symbol, timeframe);
      res.json(psychology);
    } catch (error) {
      logger.error(`Error fetching psychology for ${req.params.base}/${req.params.quote}:`, { error: error.message });
      res.status(500).json({ error: 'Failed to fetch market psychology' });
    }
  }

  static async getMarketAnalysis(req, res) {
    try {
      const symbol = MarketController.getSymbol(req);
      const timeframe = MarketController.getTimeframe(req);
      logger.info(`Fetching analysis for symbol: ${symbol}, timeframe: ${timeframe}`);
      const analysis = await MarketController.service.getMarketAnalysis(symbol, timeframe);
      res.json(analysis);
    } catch (error) {
      logger.error(`Error fetching analysis for ${req.params.base}/${req.params.quote}:`, { error: error.message });
      res.status(500).json({ error: 'Failed to fetch market analysis' });
    }
  }

  static async getAllImpactNews(req, res) {
    try {
      const timeframe = MarketController.getTimeframe(req);
      logger.info(`Fetching all high-impact news for today (timeframe: ${timeframe})`);
      const allImpactNews = await MarketController.service.getAllImpactNews(timeframe);
      res.json(allImpactNews);
    } catch (error) {
      logger.error('Error fetching all high-impact news for today:', { error: error.message });
      res.status(500).json({ error: 'Failed to fetch high-impact news for today' });
    }
  }
}

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3005;

// Middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      scriptSrc: ["'self'"],
      connectSrc: ["'self'", "api.binance.com", "api-m.sandbox.paypal.com"]
    }
  },
  hsts: {
    maxAge: 31536000,
    includeSubDomains: true,
    preload: true
  }
}));
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
  message: { error: 'Too many requests, please try again later.' }
});
app.use('/markets', limiter);
app.use('/notifications', limiter);

// JWT Authentication Middleware
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) {
    return res.status(401).json({ error: 'Authentication token missing' });
  }

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    req.user = user;
    next();
  });
};

// Subscription Access Middleware
const checkSubscriptionAccess = async (req, res, next) => {
  const { base, quote } = req.params;
  const timeframe = MarketController.getTimeframe(req);
  const symbol = `${base?.toUpperCase()}/${quote?.toUpperCase()}`;
  const endpoint = req.path.split('/').pop(); // Get the last part of the path (e.g., 'news', 'sentiment')

  // Get user subscription from DB
  const mongoClient = MongoDBClient.getInstance();
  const user = await mongoClient.getUserByEmail(req.user.email);
  
  // Free tier access check
  const isFreeTier = !user?.subscription?.active || user?.subscription?.plan === 'Free';
  if (isFreeTier) {
    // Check if endpoint is allowed for free tier
    if (!FREE_TIER_ACCESS.endpoints.includes(endpoint)) {
      return res.status(403).json({ error: `Upgrade to access ${endpoint} endpoint` });
    }
    // Check if timeframe is allowed for free tier
    if (!FREE_TIER_ACCESS.timeframes.includes(timeframe)) {
      return res.status(403).json({ error: `Upgrade for ${timeframe} access` });
    }
    // Check if currency is allowed for free tier
    if (!PLAN_CURRENCIES.Free.includes(symbol)) {
      return res.status(403).json({ error: `Upgrade for ${symbol} access` });
    }
    req.userSubscription = { plan: 'Free', active: false };
    return next();
  }

  // Paid tier checks
  if (!user?.subscription?.active) {
    return res.status(403).json({ error: 'Active subscription required' });
  }
  
  // Check timeframe access for paid plans
  const plan = user.subscription.plan;
  const timeframeHours = parseInt(timeframe.replace('h', ''));
  const hasTimeframeAccess = {
    Basic: timeframeHours >= 12,
    Pro: timeframeHours >= 6,
    Enterprise: true
  }[plan];
  
  if (!hasTimeframeAccess) {
    return res.status(403).json({ error: `Upgrade for ${timeframe} access` });
  }
  
  // Check currency access for paid plans
  const allowedCurrencies = PLAN_CURRENCIES[plan];
  if (allowedCurrencies !== 'all' && !allowedCurrencies.includes(symbol)) {
    return res.status(403).json({ error: `Upgrade for ${symbol} access` });
  }
  
  req.userSubscription = user.subscription;
  next();
};

// Validation middleware factory
const validateMarketRoute = [
  param('base').isAlphanumeric().escape(),
  param('quote').isAlphanumeric().escape(),
  query('timeframe').optional().isIn(TIMEFRAMES),
  (req, res, next) => {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }
    next();
  }
];

// Google OAuth2 Authentication
app.post('/auth/google', async (req, res) => {
  try {
    const { token } = req.body;
    const ticket = await googleClient.verifyIdToken({
      idToken: token,
      audience: GOOGLE_CLIENT_ID,
    });
    const payload = ticket.getPayload();
    if (!payload) throw new Error('Invalid token');

    const user = {
      id: uuidv4(),
      name: payload.name || 'Unknown User',
      email: payload.email || '',
      avatar: payload.picture || 'https://images.pexels.com/photos/614810/pexels-photo-614810.jpeg?auto=compress&cs=tinysrgb&w=100&h=100&dpr=2',
      availableBalance: 0,
      totalWithdrawn: 0,
      pendingWithdrawals: 0,
      subscription: {
        plan: 'Free',
        subscriptionId: null,
        startDate: null,
        active: false
      }
    };

    const mongoClient = MongoDBClient.getInstance();
    await mongoClient.createOrUpdateUser(user);

    const jwtToken = jwt.sign({ id: user.id, email: user.email }, JWT_SECRET, { expiresIn: '1h' });
    res.json({ success: true, user, token: jwtToken });
  } catch (error) {
    logger.error('Google login failed:', { error: error.message });
    res.status(401).json({ success: false, message: 'Invalid token' });
  }
});

// Subscription Route
/* ==========  PAYPAL $49 SUBSCRIPTION BACKEND  ========== */

const basicAuth = Buffer.from(`${PAYPAL_CLIENT_ID}:${PAYPAL_SECRET}`).toString('base64');

async function paypalRequest(path, method = 'GET', data = null) {
  const token = await getPayPalAccessToken();
  return axios({
    url: `${process.env.PAYPAL_API_BASE}${path}`,
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`
    },
    data
  });
}

async function getPayPalAccessToken() {
  const { data } = await axios({
    url: `${process.env.PAYPAL_API_BASE}/v1/oauth2/token`,
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Accept-Language': 'en_US',
      Authorization: `Basic ${basicAuth}`
    },
    params: { grant_type: 'client_credentials' }
  });
  return data.access_token;
}

/* ---- 1.  create $49 plan (run ONCE manually) ---- */
app.post('/create-plan', async (_req, res) => {
  if (process.env.PAYPAL_PLAN_ID) return res.json({ planId: process.env.PAYPAL_PLAN_ID });

  try {
    const payload = {
      product_id: 'PROD-PREMIUM-MONTHLY',   // you can create a product first if you want
      name: 'Premium Monthly',
      description: '$49 USD every month',
      billing_cycles: [{
        frequency: { interval_unit: 'MONTH', interval_count: 1 },
        tenure_type: 'REGULAR',
        sequence: 1,
        total_cycles: 0,
        pricing_scheme: { fixed_price: { currency_code: 'USD', value: '49.00' } }
      }],
      payment_preferences: {
        auto_bill_outstanding: true,
        setup_fee_failure_action: 'CANCEL',
        payment_failure_threshold: 3
      }
    };
    const { data } = await paypalRequest('/v1/billing/plans', 'POST', payload);
    // persist it so we never create duplicates
    require('fs').appendFileSync('.env', `\nPAYPAL_PLAN_ID=${data.id}\n`);
    process.env.PAYPAL_PLAN_ID = data.id;
    res.json({ planId: data.id });
  } catch (e) {
    logger.error('Plan creation failed:', e.response?.data || e.message);
    res.status(500).json({ error: 'Plan creation failed' });
  }
});

/* ---- 2.  create subscription for customer ---- */
app.post('/subscribe', authenticateToken, async (req, res) => {
  const { plan, return_url, cancel_url } = req.body;   // plan is "Basic" (==$49)
  if (!plan || !return_url || !cancel_url) {
    return res.status(400).json({ error: 'plan, return_url and cancel_url required' });
  }
  const PLAN_ID = process.env.PAYPAL_PLAN_ID;
  if (!PLAN_ID) return res.status(500).json({ error: 'PayPal plan not initialised' });

  try {
    const mongo = MongoDBClient.getInstance();
    const user = await mongo.getUserByEmail(req.user.email);
    if (!user) return res.status(404).json({ error: 'User not found' });

    const { data } = await paypalRequest('/v1/billing/subscriptions', 'POST', {
      plan_id: PLAN_ID,
      subscriber: { email_address: req.user.email, name: { given_name: user.name } },
      application_context: {
        brand_name: 'YourSite',
        return_url,
        cancel_url
      }
    });

    // front-end redirects buyer to approval link
    const approvalLink = data.links.find(l => l.rel === 'approve').href;
    res.json({ subscriptionId: data.id, approvalLink });
  } catch (e) {
    logger.error('Subscription creation failed:', e.response?.data || e.message);
    res.status(500).json({ error: 'Failed to create subscription' });
  }
});

/* ---- 3.  webhook: activate user after first payment ---- */
app.post('/webhook', bodyParser.raw({ type: 'application/json' }), (req, res) => {
  const sig = req.headers['paypal-transmission-sig'];
  const ts  = req.headers['paypal-transmission-time'];
  const tx  = req.headers['paypal-transmission-id'];
  const expected = require('crypto')
    .createHash('sha256')
    .update(`${tx}|${process.env.WEBHOOK_SECRET}|${ts}|${req.body.toString()}`)
    .digest('hex');
  if (sig !== expected) return res.status(400).send('Bad signature');

  const event = JSON.parse(req.body);
  if (event.event_type === 'BILLING.SUBSCRIPTION.ACTIVATED') {
    const subId = event.resource.id;
    const mongo = MongoDBClient.getInstance();
    mongo.usersCollection.updateOne(
      { 'subscription.subscriptionId': subId },
      { $set: { 'subscription.active': true, 'subscription.plan': 'Basic' } }
    ).then(() => logger.info(`✅ User activated for sub ${subId}`));
  }
  res.status(200).send('OK');
});
/* ========================================================= */
// User Profile Route
app.get('/api/profile', authenticateToken, async (req, res) => {
  try {
    const userEmail = req.user.email;
    const mongoClient = MongoDBClient.getInstance();
    const user = await mongoClient.getUserByEmail(userEmail);
    if (user) {
      res.json(user);
    } else {
      res.status(404).json({ error: 'User not found' });
    }
  } catch (error) {
    logger.error('Error fetching profile:', { error: error.message });
    res.status(500).json({ error: 'Failed to fetch profile' });
  }
});

// Health Checks
app.get('/health', async (req, res) => {
  try {
    await MongoDBClient.getInstance().client.db(DB_NAME).admin().ping();
    res.json({ 
      status: 'healthy', 
      timestamp: new Date().toISOString(),
      uptime: process.uptime()
    });
  } catch (error) {
    logger.error('Health check failed:', { error: error.message });
    res.status(503).json({ status: 'unhealthy', error: error.message });
  }
});

app.get('/ready', (req, res) => {
  res.json({ status: 'ready' });
});

// Metrics endpoint
app.get('/metrics', (req, res) => {
  res.json({
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    requests: global.requestCount || 0,
    timestamp: new Date().toISOString()
  });
});

// Track requests
app.use((req, res, next) => {
  global.requestCount = (global.requestCount || 0) + 1;
  next();
});

// Market Data Routes
app.get('/markets', MarketController.getAllMarkets);
app.get('/markets/:base/:quote', validateMarketRoute, authenticateToken, checkSubscriptionAccess, MarketController.getMarketBySymbol);
app.get('/markets/:base/:quote/rate', validateMarketRoute, authenticateToken, checkSubscriptionAccess, MarketController.getCurrencyRate);
app.get('/markets/:base/:quote/strategies', validateMarketRoute, authenticateToken, checkSubscriptionAccess, MarketController.getTradingStrategies);
app.get('/markets/:base/:quote/news', validateMarketRoute, authenticateToken, checkSubscriptionAccess, MarketController.getMarketNews);
app.get('/markets/:base/:quote/sentiment', validateMarketRoute, authenticateToken, checkSubscriptionAccess, MarketController.getMarketSentiment);
app.get('/markets/:base/:quote/psychology', validateMarketRoute, authenticateToken, checkSubscriptionAccess, MarketController.getMarketPsychology);
app.get('/markets/:base/:quote/analysis', validateMarketRoute, authenticateToken, checkSubscriptionAccess, MarketController.getMarketAnalysis);
app.get('/notifications/all-impact-news', authenticateToken, MarketController.getAllImpactNews);

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('Unhandled error:', { error: err.stack, path: req.path });
  res.status(500).json({ error: 'Something went wrong!' });
});

// Start server
const server = app.listen(PORT, () => {
  logger.info(`Server is running on port ${PORT} in ${NODE_ENV} mode`);
});

// Graceful shutdown
const gracefulShutdown = async () => {
  logger.info('Shutting down gracefully');
  try {
    if (mongoInstance) {
      await mongoInstance.client.close();
      logger.info('MongoDB connection closed');
    }
    if (MarketController.service.binanceClient.ws) {
      MarketController.service.binanceClient.ws.close();
      logger.info('Binance WebSocket closed');
    }
    server.close(() => {
      logger.info('HTTP server closed');
      process.exit(0);
    });
  } catch (error) {
    logger.error('Error during shutdown:', { error: error.message });
    process.exit(1);
  }
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', { promise, reason });
});