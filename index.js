const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const http = require('http');
const WebSocket = require('ws');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Create HTTP server (for Express REST & WebSocket)
const server = http.createServer(app);

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Track active WebSocket connections
const wsClients = new Map(); // userId -> ws connection

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

  // 1. Create table if it doesn't exist
  await pool.query(`
    CREATE TABLE IF NOT EXISTS game_accounts (
      user_id TEXT NOT NULL,
      account_name TEXT NOT NULL,
      stats JSONB NOT NULL DEFAULT '{"gold": 0, "xp": 0, "level": 1}'::jsonb,
      inventory JSONB NOT NULL DEFAULT '[]'::jsonb,
      progress JSONB NOT NULL DEFAULT '{}'::jsonb,
      is_online BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (user_id, account_name)
    )
  `);

  // 2. Add the column to existing tables if it's missing
  await pool.query(`
    ALTER TABLE game_accounts 
    ADD COLUMN IF NOT EXISTS is_online BOOLEAN NOT NULL DEFAULT false;
  `);
}

function serializeAccount(row) {
  return {
    accountName: row.account_name,
    isOnline: row.is_online ?? false,
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

// Broadcast message across connected WebSocket clients
function broadcastMessage(messageObj) {
  const payload = JSON.stringify(messageObj);

  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

// =====================================
// WEBSOCKET HANDLERS
// =====================================

wss.on('connection', (ws) => {
  console.log('🔌 New WebSocket connection');
  let userId = null;
  let accountName = null;

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

        // 1. Authenticate Client
        if (message.type === 'auth') {
          userId = message.userId;
          accountName = message.accountName || null;
        
          // Immediately close any existing socket for this user (handles page refreshes)
          if (wsClients.has(userId)) {
            const existingWs = wsClients.get(userId);
            if (existingWs && existingWs !== ws) {
              existingWs.close();
            }
          }
        
          wsClients.set(userId, ws);
          
          // Suppress logging if accountName isn't ready yet
          if (accountName) {
            console.log(`✅ [WS] Account "${accountName}" (${userId}) authenticated`);
          } else {
            console.log(`✅ [WS] User (${userId}) connected (awaiting account sync)`);
          }
        
          ws.send(JSON.stringify({ type: 'auth_success', message: 'Connected to WebSocket server' }));
          return;
        }

      // Fallback: capture accountName from sync or status packets if missing in initial auth
      if (message.data?.account_name) accountName = message.data.account_name;
      if (message.accountName) accountName = message.accountName;

      // 2. Client Remote Logger
      if (message.type === 'log') {
        console.log(`📝 [LOG] [${message.data?.level || 'INFO'}] ${message.data?.message}`);
        return;
      }

      // 3. Drop Notifications
      if (message.type === 'drop_notification') {
        console.log(`🎁 [DROP] ${message.data?.accountName}: ${message.data?.item?.name}`);
        return;
      }

      // 4. Status Update (Online / Offline)
      if (message.type === 'status_update') {
        const { status } = message;
        const isOnline = status === 'online';
        const targetUserId = process.env.CREATOR_GITHUB_ID 
          ? `github_${process.env.CREATOR_GITHUB_ID}` 
          : (message.userId || userId);

        if (accountName && targetUserId) {
          console.log(`🟢 [WS STATUS] Setting "${accountName}" to ${status} for User ${targetUserId}`);

          await pool.query(`
            UPDATE game_accounts 
            SET is_online = $1, updated_at = NOW() 
            WHERE (user_id = $2 OR user_id = $3) AND LOWER(account_name) = LOWER($4)
          `, [isOnline, targetUserId, userId, accountName]);

          broadcastMessage({
            type: 'status_changed',
            userId: targetUserId,
            accountName,
            isOnline,
            timestamp: new Date().toISOString()
          });
        }
        return;
      }

      // 5. Game Data Sync Packet
      if (message.type === 'sync_account') {
        const accountData = message.data || {};
        const { account_name, stats, inventory, progress, online } = accountData;

        const targetUserId = process.env.CREATOR_GITHUB_ID 
          ? `github_${process.env.CREATOR_GITHUB_ID}` 
          : (message.userId || userId);

        if (!account_name || !targetUserId) {
          console.error('❌ [WS SYNC] Missing account_name or userId');
          return;
        }

        accountName = account_name;

        console.log(`📊 [WS SYNC] Saving account "${account_name}" for User ${targetUserId}...`);

        const isOnline = online ?? true;

        await pool.query(`
          INSERT INTO game_accounts (user_id, account_name, stats, inventory, progress, is_online)
          VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6)
          ON CONFLICT (user_id, account_name)
          DO UPDATE SET
            stats = EXCLUDED.stats,
            inventory = EXCLUDED.inventory,
            progress = EXCLUDED.progress,
            is_online = EXCLUDED.is_online,
            updated_at = NOW()
        `, [
          targetUserId,
          account_name,
          JSON.stringify(stats || DEFAULT_STATS),
          JSON.stringify(inventory || []),
          JSON.stringify(progress || {}),
          isOnline
        ]);

        console.log(`💾 [DB SUCCESS] Updated database for "${account_name}" under ${targetUserId}`);
        
        ws.send(JSON.stringify({
          type: 'sync_success',
          accountName: account_name,
          timestamp: new Date().toISOString()
        }));

        broadcastMessage({
          type: 'account_updated',
          userId: targetUserId,
          accountName: account_name,
          isOnline,
          stats,
          timestamp: new Date().toISOString()
        });
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

  // Handle Connection Close
  ws.on('close', async () => {
    const cleanUserId = userId ? userId.replace(/^github_/, '') : null;
    const targetUserId = process.env.CREATOR_GITHUB_ID 
      ? `github_${process.env.CREATOR_GITHUB_ID}` 
      : userId;

    if (accountName && (targetUserId || cleanUserId)) {
      console.log(`❌ [WS] Account "${accountName}" (${targetUserId || cleanUserId}) disconnected`);

      try {
        const result = await pool.query(`
          UPDATE game_accounts 
          SET is_online = false, updated_at = NOW() 
          WHERE (user_id = $1 OR user_id = $2 OR user_id = $3) 
            AND LOWER(account_name) = LOWER($4)
        `, [targetUserId, cleanUserId, userId, accountName]);

        console.log(`📉 [DB DISCONNECT] Updated ${result.rowCount} row(s) to offline for "${accountName}"`);

        broadcastMessage({
          type: 'status_changed',
          userId: targetUserId || userId,
          accountName,
          isOnline: false,
          timestamp: new Date().toISOString()
        });
      } catch (err) {
        console.error('Failed to update disconnect status in DB:', err);
      }
    } else if (userId) {
      console.log(`❌ [WS] User ${userId} disconnected`);
    }

    if (userId) {
      wsClients.delete(userId);
    }
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });
});

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
      'WS / (WebSocket connection)'
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
      SELECT account_name, stats, inventory, progress, is_online, created_at, updated_at
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
  const { accountName, stats, inventory, progress, isOnline } = req.body;

  if (!accountName) {
    return res.status(400).json({ error: 'accountName is required' });
  }

  const accountStats = stats || DEFAULT_STATS;
  const accountInventory = inventory || [];
  const accountProgress = progress || {};
  const onlineState = isOnline ?? false;

  try {
    const existing = await pool.query(`
      SELECT 1
      FROM game_accounts
      WHERE user_id = $1 AND account_name = $2
    `, [userId, accountName]);

    const result = await pool.query(`
      INSERT INTO game_accounts (
        user_id, account_name, stats, inventory, progress, is_online
      )
      VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6)
      ON CONFLICT (user_id, account_name)
      DO UPDATE SET
        stats = EXCLUDED.stats,
        inventory = EXCLUDED.inventory,
        progress = EXCLUDED.progress,
        is_online = EXCLUDED.is_online,
        updated_at = NOW()
      RETURNING account_name, stats, inventory, progress, is_online, created_at, updated_at
    `, [
      userId,
      accountName,
      JSON.stringify(accountStats),
      JSON.stringify(accountInventory),
      JSON.stringify(accountProgress),
      onlineState
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
      RETURNING account_name, stats, inventory, progress, is_online, created_at, updated_at
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
