const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const http = require('http');
const WebSocket = require('ws');
const { Listener, PacketPriority, PacketReliability } = require('raknet-native');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;
const RAKNET_PORT = process.env.RAKNET_PORT || 19132;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Create HTTP server (for REST API and WebSocket upgrade)
const server = http.createServer(app);

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Store connected WebSocket and RakNet clients
const wsClients = new Map();     // userId -> ws
const raknetClients = new Map(); // userId -> raknetConnection

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

// Unified Broadcast for WS and RakNet
function broadcastMessage(messageObj) {
  const payload = JSON.stringify(messageObj);

  // Send to all connected WebSocket clients
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });

  // Send to all connected RakNet clients
  for (const [, connection] of raknetClients.entries()) {
    connection.send(
      Buffer.from(payload),
      PacketPriority.MEDIUM_PRIORITY,
      PacketReliability.RELIABLE_ORDERED,
      0
    );
  }
}

// =====================================
// WEBSOCKET SERVER HANDLERS
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
// RAKNET SERVER HANDLERS
// =====================================

function startRakNetServer() {
  const raknetListener = new Listener();

  raknetListener.on('pong', (addr, ping) => {
    console.log(`📡 RakNet Ping received from ${addr}`);
  });

  raknetListener.listen('0.0.0.0', RAKNET_PORT);
  console.log(`🚀 RakNet listener bound to UDP port ${RAKNET_PORT}`);

  raknetListener.on('openConnection', (connection) => {
    console.log(`🔌 New RakNet connection from ${connection.address}`);
    let userId = null;

    connection.on('encapsulated', async (packet) => {
      try {
        const rawString = packet.buffer.toString('utf-8');
        const message = JSON.parse(rawString);

        if (message.type === 'auth') {
          userId = message.userId;
          raknetClients.set(userId, connection);
          console.log(`✅ [RakNet] User ${userId} authenticated`);

          const ack = Buffer.from(JSON.stringify({ type: 'auth_success', message: 'Connected to RakNet server' }));
          connection.send(ack, PacketPriority.IMMEDIATE_PRIORITY, PacketReliability.RELIABLE_ORDERED, 0);
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
          connection.send(pong, PacketPriority.HIGH_PRIORITY, PacketReliability.UNRELIABLE, 0);
        }
      } catch (err) {
        console.error('RakNet packet decode error:', err);
      }
    });

    connection.on('close', () => {
      if (userId) {
        raknetClients.delete(userId);
        console.log(`❌ [RakNet] User ${userId} disconnected`);
      }
    });
  });
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

// OAuth, Accounts, and Math calculation endpoints unchanged...
app.post('/api/auth/github/callback', async (req, res) => {
  const { code } = req.body;
  if (!code) return res.status(400).json({ error: 'code is required' });
  if (!GITHUB_CLIENT_ID || !GITHUB_CLIENT_SECRET) {
    return res.status(500).json({ error: 'GitHub OAuth is not configured on the server' });
  }

  try {
    const tokenResponse = await fetch('https://github.com/login/oauth/access_token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify({ client_id: GITHUB_CLIENT_ID, client_secret: GITHUB_CLIENT_SECRET, code })
    });
    const tokenData = await tokenResponse.json();
    if (tokenData.error || !tokenData.access_token) {
      return res.status(400).json({ error: tokenData.error_description || 'Failed token exchange' });
    }

    const profileResponse = await fetch('https://api.github.com/user', {
      headers: { 'Authorization': `Bearer ${tokenData.access_token}`, 'User-Agent': 'game-tracker-backend' }
    });
    const githubUser = await profileResponse.json();

    res.json({ success: true, user: { id: githubUser.id, login: githubUser.login, avatar: githubUser.avatar_url } });
  } catch (error) {
    res.status(500).json({ error: 'GitHub authentication failed' });
  }
});

app.get('/api/accounts/:userId', async (req, res) => {
  const { userId } = req.params;
  try {
    const result = await pool.query(
      `SELECT account_name, stats, inventory, progress, created_at, updated_at FROM game_accounts WHERE user_id = $1 ORDER BY created_at ASC`, 
      [userId]
    );
    res.json({ userId, accounts: result.rows.map(serializeAccount), count: result.rows.length });
  } catch (error) {
    databaseError(res, error);
  }
});

app.post('/api/accounts/:userId', async (req, res) => {
  const { userId } = req.params;
  const { accountName, stats, inventory, progress } = req.body;

  if (!accountName) return res.status(400).json({ error: 'accountName is required' });

  try {
    const result = await pool.query(`
      INSERT INTO game_accounts (user_id, account_name, stats, inventory, progress)
      VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb)
      ON CONFLICT (user_id, account_name) DO UPDATE SET
        stats = EXCLUDED.stats, inventory = EXCLUDED.inventory, progress = EXCLUDED.progress, updated_at = NOW()
      RETURNING account_name, stats, inventory, progress, created_at, updated_at
    `, [userId, accountName, JSON.stringify(stats || DEFAULT_STATS), JSON.stringify(inventory || []), JSON.stringify(progress || {})]);

    res.json({ success: true, account: serializeAccount(result.rows[0]) });
  } catch (error) {
    databaseError(res, error);
  }
});

app.delete('/api/accounts/:userId/:accountName', async (req, res) => {
  const { userId, accountName } = req.params;
  try {
    const deleted = await pool.query(
      `DELETE FROM game_accounts WHERE user_id = $1 AND account_name = $2 RETURNING account_name, stats, inventory, progress, created_at, updated_at`,
      [userId, accountName]
    );
    if (deleted.rowCount === 0) return res.status(404).json({ error: 'Account not found' });
    res.json({ success: true, deletedAccount: serializeAccount(deleted.rows[0]) });
  } catch (error) {
    databaseError(res, error);
  }
});

// =====================================
// START SERVER
// =====================================

async function startServer() {
  try {
    await initializeDatabase();

    // Start HTTP & WebSocket Server
    server.listen(PORT, '0.0.0.0', () => {
      console.log(`🎮 Game Tracker API running on http://localhost:${PORT}`);
      console.log(`📡 WebSocket server running`);
    });

    // Start RakNet Listener
    startRakNetServer();

  } catch (error) {
    console.error('Unable to initialize the database:', error.message);
    process.exitCode = 1;
  }
}

startServer();
