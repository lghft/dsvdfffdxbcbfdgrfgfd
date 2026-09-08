const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const http = require('http');
const WebSocket = require('ws');
require('dotenv').config();

let raknetModule = null;
try {
  raknetModule = require('@bedrock-protocol/raknet');
} catch (e) {
  console.warn('⚠️ RakNet package not loaded:', e.message);
}

const app = express();
const PORT = process.env.PORT || 3001;
const RAKNET_PORT = process.env.RAKNET_PORT || 19132;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Create HTTP server
const server = http.createServer(app);

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Track active connections
const wsClients = new Map();     // userId -> ws connection
const raknetClients = new Map(); // userId -> raknet connection

let raknetServer = null;

// GitHub OAuth Configuration
const GITHUB_CLIENT_ID = process.env.GITHUB_CLIENT_ID;
const GITHUB_CLIENT_SECRET = process.env.GITHUB_CLIENT_SECRET;

// Middleware
app.use(cors());
app.use(express.json());

// =====================================
// DATABASE & HELPERS
// =====================================
const DEFAULT_STATS = { gold: 0, xp: 0, level: 1 };

async function initializeDatabase() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required to start the API');
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS game_accounts (
      user_id TEXT NOT NULL,
      account_name TEXT NOT NULL,
      stats JSONB NOT NULL DEFAULT '{"gold": 0, "xp": 0, "level": 1}'::jsonb,
      inventory JSONB NOT NULL DEFAULT '[]'::jsonb,
      progress JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (user_id, account_name)
    )
  `);
}

function serializeAccount(row) {
  return {
    accountName: row.account_name,
    stats: row.stats,
    inventory: row.inventory,
    progress: row.progress,
    updatedAt: row.updated_at.toISOString(),
    createdAt: row.created_at.toISOString()
  };
}

function databaseError(res, error) {
  console.error('Database operation failed:', error);
  return res.status(500).json({ error: 'Database operation failed' });
}

// Unified Broadcast across WebSocket and RakNet
function broadcastMessage(messageObj) {
  const payload = JSON.stringify(messageObj);

  // Send to WebSocket clients
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });

  // Send to RakNet clients
  for (const [userId, clientConnection] of raknetClients.entries()) {
    try {
      if (clientConnection && typeof clientConnection.send === 'function') {
        clientConnection.send(Buffer.from(payload));
      }
    } catch (err) {
      console.error(`Failed to send RakNet message to user ${userId}:`, err);
    }
  }
}

// =====================================
// WEBSOCKET HANDLERS
// =====================================

wss.on('connection', (ws) => {
  console.log('🔌 New WebSocket connection');
  let userId = null;

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      if (message.type === 'auth') {
        userId = message.userId;
        wsClients.set(userId, ws);
        console.log(`✅ [WS] User ${userId} authenticated`);
        ws.send(JSON.stringify({ type: 'auth_success', message: 'Connected to WebSocket server' }));
        return;
      }

      if (message.type === 'stats_update') {
        console.log(`📊 [WS] Stats update from ${userId}:`, message.data);
        broadcastMessage({
          type: 'stats_update',
          userId,
          data: message.data,
          timestamp: new Date().toISOString()
        });
      }

      if (message.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (error) {
      console.error('WebSocket message error:', error);
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid message format' }));
    }
  });

  ws.on('close', () => {
    if (userId) {
      wsClients.delete(userId);
      console.log(`❌ [WS] User ${userId} disconnected`);
    }
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });
});

// =====================================
// RAKNET HANDLERS
// =====================================

function startRakNetServer() {
  if (!raknetModule || !raknetModule.Server) {
    console.warn('⚠️ RakNet protocol module unavailable. Running in HTTP/WebSocket mode only.');
    return;
  }

  try {
    raknetServer = new raknetModule.Server({
      host: '0.0.0.0',
      port: Number(RAKNET_PORT)
    });

    raknetServer.on('openConnection', (client) => {
      console.log('🔌 New RakNet connection established');
      let userId = null;

      client.on('encapsulated', (packet) => {
        try {
          const rawString = packet.buffer ? packet.buffer.toString('utf-8') : packet.toString('utf-8');
          const message = JSON.parse(rawString);

          if (message.type === 'auth') {
            userId = message.userId;
            raknetClients.set(userId, client);
            console.log(`✅ [RakNet] User ${userId} authenticated`);

            const ack = Buffer.from(JSON.stringify({ type: 'auth_success', message: 'Connected to RakNet server' }));
            client.send(ack);
            return;
          }

          if (message.type === 'stats_update') {
            console.log(`📊 [RakNet] Stats update from ${userId}:`, message.data);
            broadcastMessage({
              type: 'stats_update',
              userId,
              data: message.data,
              timestamp: new Date().toISOString()
            });
          }

          if (message.type === 'ping') {
            const pong = Buffer.from(JSON.stringify({ type: 'pong' }));
            client.send(pong);
          }
        } catch (err) {
          console.error('RakNet packet decode error:', err);
        }
      });

      client.on('close', () => {
        if (userId) {
          raknetClients.delete(userId);
          console.log(`❌ [RakNet] User ${userId} disconnected`);
        }
      });
    });

    raknetServer.listen();
    console.log(`🚀 RakNet UDP server listening on port ${RAKNET_PORT}`);
  } catch (err) {
    console.error('RakNet startup warning:', err.message);
  }
}

// =====================================
// REST ROUTES
// =====================================

app.get('/', (req, res) => {
  res.json({ 
    message: 'Game Tracker API is running!',
    version: '1.0.0',
    endpoints: [
      'GET /api/health',
      'GET /api/accounts/:userId',
      'POST /api/accounts/:userId',
      'DELETE /api/accounts/:userId/:accountName',
      'POST /api/calculate-pot',
      'POST /api/calculate-upgrades',
      'WS / (WebSocket connection)',
      `UDP :${RAKNET_PORT} (RakNet connection)`
    ]
  });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date() });
});

app.post('/api/auth/github/callback', async (req, res) => {
  const { code } = req.body;

  if (!code) {
    return res.status(400).json({ error: 'code is required' });
  }

  if (!GITHUB_CLIENT_ID || !GITHUB_CLIENT_SECRET) {
    console.error('GITHUB_CLIENT_ID / GITHUB_CLIENT_SECRET not configured');
    return res.status(500).json({ error: 'GitHub OAuth is not configured on the server' });
  }

  try {
    const tokenResponse = await fetch('https://github.com/login/oauth/access_token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify({
        client_id: GITHUB_CLIENT_ID,
        client_secret: GITHUB_CLIENT_SECRET,
        code
      })
    });

    const tokenData = await tokenResponse.json();

    if (tokenData.error || !tokenData.access_token) {
      console.error('GitHub token exchange failed:', tokenData);
      return res.status(400).json({
        error: tokenData.error_description || 'Failed to exchange code for token'
      });
    }

    const profileResponse = await fetch('https://api.github.com/user', {
      headers: {
        'Authorization': `Bearer ${tokenData.access_token}`,
        'User-Agent': 'game-tracker-backend'
      }
    });

    if (!profileResponse.ok) {
      console.error('GitHub profile fetch failed:', profileResponse.status);
      return res.status(502).json({ error: 'Failed to fetch GitHub profile' });
    }

    const githubUser = await profileResponse.json();

    if (process.env.CREATOR_GITHUB_ID && githubUser.id.toString() !== process.env.CREATOR_GITHUB_ID) {
      console.error(`Access denied for user ${githubUser.login} (ID: ${githubUser.id}). Creator ID: ${process.env.CREATOR_GITHUB_ID}`);
      return res.status(403).json({
        error: 'Access denied. Only the repository creator can use this app.'
      });
    }

    res.json({
      success: true,
      user: {
        id: githubUser.id,
        login: githubUser.login,
        avatar: githubUser.avatar_url
      }
    });
  } catch (error) {
    console.error('GitHub OAuth error:', error);
    res.status(500).json({ error: 'GitHub authentication failed' });
  }
});

app.get('/api/accounts/:userId', async (req, res) => {
  const { userId } = req.params;

  try {
    const result = await pool.query(`
      SELECT account_name, stats, inventory, progress, created_at, updated_at
      FROM game_accounts
      WHERE user_id = $1
      ORDER BY created_at ASC, account_name ASC
    `, [userId]);
    const userAccounts = result.rows.map(serializeAccount);

    res.json({
      userId,
      accounts: userAccounts,
      count: userAccounts.length
    });
  } catch (error) {
    databaseError(res, error);
  }
});

app.post('/api/accounts/:userId', async (req, res) => {
  const { userId } = req.params;
  const { accountName, stats, inventory, progress } = req.body;

  if (!accountName) {
    return res.status(400).json({ error: 'accountName is required' });
  }

  const accountStats = stats || DEFAULT_STATS;
  const accountInventory = inventory || [];
  const accountProgress = progress || {};

  try {
    const existing = await pool.query(`
      SELECT 1
      FROM game_accounts
      WHERE user_id = $1 AND account_name = $2
    `, [userId, accountName]);

    const result = await pool.query(`
      INSERT INTO game_accounts (
        user_id, account_name, stats, inventory, progress
      )
      VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb)
      ON CONFLICT (user_id, account_name)
      DO UPDATE SET
        stats = EXCLUDED.stats,
        inventory = EXCLUDED.inventory,
        progress = EXCLUDED.progress,
        updated_at = NOW()
      RETURNING account_name, stats, inventory, progress, created_at, updated_at
    `, [
      userId,
      accountName,
      JSON.stringify(accountStats),
      JSON.stringify(accountInventory),
      JSON.stringify(accountProgress)
    ]);

    const wasUpdated = existing.rowCount > 0;
    res.status(wasUpdated ? 200 : 201).json({
      success: true,
      message: `Account "${accountName}" ${wasUpdated ? 'updated' : 'created'}`,
      account: serializeAccount(result.rows[0])
    });
  } catch (error) {
    databaseError(res, error);
  }
});

app.delete('/api/accounts/:userId/:accountName', async (req, res) => {
  const { userId, accountName } = req.params;

  try {
    const deleted = await pool.query(`
      DELETE FROM game_accounts
      WHERE user_id = $1 AND account_name = $2
      RETURNING account_name, stats, inventory, progress, created_at, updated_at
    `, [userId, accountName]);

    if (deleted.rowCount === 0) {
      const user = await pool.query(`
        SELECT 1
        FROM game_accounts
        WHERE user_id = $1
        LIMIT 1
      `, [userId]);

      if (user.rowCount === 0) {
        return res.status(404).json({ error: 'User not found' });
      }

      return res.status(404).json({ error: 'Account not found' });
    }

    res.json({
      success: true,
      message: `Account "${accountName}" deleted`,
      deletedAccount: serializeAccount(deleted.rows[0])
    });
  } catch (error) {
    databaseError(res, error);
  }
});

app.post('/api/calculate-pot', (req, res) => {
  const { items } = req.body;

  if (!Array.isArray(items)) {
    return res.status(400).json({ error: 'items must be an array' });
  }

  const rarityMultipliers = {
    'common': 1,
    'uncommon': 5,
    'rare': 25,
    'epic': 125,
    'legendary': 625
  };

  let totalPot = 0;
  const breakdown = {};

  items.forEach(item => {
    const rarity = item.rarity || 'common';
    const baseMultiplier = rarityMultipliers[rarity] || 1;

    let itemValue = 0;

    if (item.sellPrice) {
      itemValue = item.sellPrice;
    } else {
      const upgradeBonus = (item.currentUpgrade || 0) * 1.5;
      itemValue = baseMultiplier * (1 + upgradeBonus);
    }

    totalPot += itemValue;

    if (!breakdown[rarity]) {
      breakdown[rarity] = { count: 0, total: 0 };
    }
    breakdown[rarity].count += 1;
    breakdown[rarity].total += itemValue;
  });

  res.json({
    totalPot: Math.floor(totalPot),
    itemCount: items.length,
    breakdown,
    calculatedAt: new Date().toISOString()
  });
});

app.post('/api/calculate-upgrades', (req, res) => {
  const { items } = req.body;

  if (!Array.isArray(items)) {
    return res.status(400).json({ error: 'items must be an array' });
  }

  const upgradeable = items
    .filter(item => item.currentUpgrade && item.currentUpgrade < item.maxUpgrades)
    .map(item => {
      const baseUpgradeCost = item.sellPrice ? item.sellPrice * 0.75 : 100;
      const costPerLevel = Math.ceil(baseUpgradeCost * (item.currentUpgrade + 1));
      const costToMax = costPerLevel * (item.maxUpgrades - item.currentUpgrade);

      return {
        id: item.id,
        name: item.name,
        rarity: item.rarity,
        currentLevel: item.currentUpgrade,
        maxLevel: item.maxUpgrades,
        levelsRemaining: item.maxUpgrades - item.currentUpgrade,
        costPerLevel,
        costToMax
      };
    })
    .sort((a, b) => a.costPerLevel - b.costPerLevel);

  const totalUpgradeCost = upgradeable.reduce((sum, item) => sum + item.costToMax, 0);

  res.json({
    upgradeableItems: upgradeable,
    count: upgradeable.length,
    totalUpgradeCost: Math.floor(totalUpgradeCost),
    calculatedAt: new Date().toISOString()
  });
});

app.use((req, res) => {
  res.status(404).json({ 
    error: 'Endpoint not found',
    path: req.path,
    method: req.method
  });
});

// =====================================
// START SERVER
// =====================================

async function startServer() {
  try {
    await initializeDatabase();

    // Start RakNet UDP Server
    startRakNetServer();

    // Start Express REST & WebSocket Server
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`🎮 Game Tracker API running on http://localhost:${PORT}`);
      console.log(`📡 CORS enabled for all origins`);
      console.log(`🔌 WebSocket server running`);
    });

  } catch (error) {
    console.error('Unable to initialize the server:', error.message);
    process.exitCode = 1;
  }
}

startServer();
