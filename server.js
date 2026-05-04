/**
 * DropBeam — Signaling Server
 * ─────────────────────────────────────────────────────────────────────────────
 * Role: Match two peers that share the same 6-digit session code.
 *       Relay WebRTC signaling messages (offer/answer/ICE) between them.
 *       Never touches the actual file data — all file bytes go P2P via WebRTC.
 *
 * Usage:
 *   npm install express socket.io cors
 *   node server.js
 *
 * Then open index.html in two browser tabs / devices.
 * Make sure they both point to this server's address.
 * ─────────────────────────────────────────────────────────────────────────────
 */

const express = require('express');
const http    = require('http');
const { Server } = require('socket.io');
const cors    = require('cors');
const path    = require('path');

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const PORT        = process.env.PORT || 3000;
const MAX_ROOM    = 2;            // Only two peers per session
const ROOM_TTL_MS = 30 * 60 * 1000; // Rooms expire after 30 minutes

// ─── APP SETUP ────────────────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: {
    origin: '*',           // For development. Restrict in production.
    methods: ['GET', 'POST'],
  },
  maxHttpBufferSize: 1e6,  // 1 MB max for signaling messages (not file data)
});

app.use(cors());
app.use(express.json());

// Optionally serve index.html directly from this server:
app.use(express.static(path.join(__dirname)));

app.get('/health', (_req, res) => res.json({ status: 'ok', uptime: process.uptime() }));

// ─── ROOM REGISTRY ────────────────────────────────────────────────────────────
/**
 * rooms: Map<roomCode, { sockets: Set<socketId>, createdAt: Date, timer: NodeJS.Timeout }>
 */
const rooms = new Map();

function createRoom(code) {
  if (rooms.has(code)) return;
  const timer = setTimeout(() => {
    expireRoom(code);
  }, ROOM_TTL_MS);
  rooms.set(code, { sockets: new Set(), createdAt: new Date(), timer });
  serverLog(`Room [${code}] created.`);
}

function expireRoom(code) {
  if (!rooms.has(code)) return;
  const room = rooms.get(code);
  clearTimeout(room.timer);
  // Notify all peers in the room
  io.to(code).emit('error', { message: 'Session expired.' });
  // Force-disconnect sockets in the room
  for (const sid of room.sockets) {
    const s = io.sockets.sockets.get(sid);
    if (s) s.leave(code);
  }
  rooms.delete(code);
  serverLog(`Room [${code}] expired.`);
}

function removeSocketFromRooms(socket) {
  for (const [code, room] of rooms.entries()) {
    if (room.sockets.has(socket.id)) {
      room.sockets.delete(socket.id);
      serverLog(`Socket ${socket.id} left room [${code}]. Peers: ${room.sockets.size}`);
      // Notify the remaining peer
      socket.to(code).emit('peer-left');
      // Clean up empty rooms
      if (room.sockets.size === 0) {
        expireRoom(code);
      }
    }
  }
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────
function serverLog(msg) {
  const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
  console.log(`[${ts}] ${msg}`);
}

function validateCode(code) {
  // Accepts "123-456" or "123456"
  return /^\d{3}-\d{3}$/.test(code) || /^\d{6}$/.test(code);
}

function normalizeCode(code) {
  // Store without dash for consistency
  return code.replace('-', '');
}

// ─── SOCKET.IO EVENTS ─────────────────────────────────────────────────────────
io.on('connection', socket => {
  serverLog(`Socket connected: ${socket.id}`);

  // ── Sender: create a room and wait ──────────────────────────────────────────
  socket.on('create-room', ({ room: rawCode }) => {
    if (!validateCode(rawCode)) {
      socket.emit('error', { message: 'Invalid session code format.' });
      return;
    }
    const code = normalizeCode(rawCode);

    if (rooms.has(code)) {
      socket.emit('error', { message: 'Session code already in use. Generate a new one.' });
      return;
    }

    createRoom(code);
    const room = rooms.get(code);
    room.sockets.add(socket.id);
    socket.join(code);
    socket.emit('room-created', { room: code });
    serverLog(`Socket ${socket.id} created room [${code}]`);
  });

  // ── Receiver: join an existing room ─────────────────────────────────────────
  socket.on('join-room', ({ room: rawCode }) => {
    if (!validateCode(rawCode)) {
      socket.emit('error', { message: 'Invalid session code format.' });
      return;
    }
    const code = normalizeCode(rawCode);

    if (!rooms.has(code)) {
      socket.emit('error', { message: 'Session not found. Check the code and try again.' });
      return;
    }

    const room = rooms.get(code);

    if (room.sockets.size >= MAX_ROOM) {
      socket.emit('error', { message: 'Session is full (2 peers already connected).' });
      return;
    }

    room.sockets.add(socket.id);
    socket.join(code);
    socket.emit('room-joined', { room: code });
    serverLog(`Socket ${socket.id} joined room [${code}]. Peers: ${room.sockets.size}`);

    // Tell the sender that a peer has arrived
    socket.to(code).emit('peer-joined');
  });

  // ── WebRTC Signal Relay ──────────────────────────────────────────────────────
  // Payload: { room, type: 'offer' | 'answer' | 'ice', data: <SDP or ICE candidate> }
  socket.on('signal', ({ room: rawCode, type, data }) => {
    if (!rawCode || !type || !data) return;
    const code = normalizeCode(rawCode);

    if (!rooms.has(code)) {
      socket.emit('error', { message: 'Room no longer exists.' });
      return;
    }

    // Forward the signal to all OTHER peers in the room (not back to sender)
    socket.to(code).emit('signal', { type, data });
    serverLog(`Signal [${type}] relayed in room [${code}]`);
  });

  // ── Disconnect cleanup ───────────────────────────────────────────────────────
  socket.on('disconnect', reason => {
    serverLog(`Socket disconnected: ${socket.id} — ${reason}`);
    removeSocketFromRooms(socket);
  });

  socket.on('error', err => {
    serverLog(`Socket error from ${socket.id}: ${err.message}`);
  });
});

// ─── START ────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════╗
║           DropBeam  Signaling Server          ║
║                                              ║
║   Listening on  http://localhost:${PORT}       ║
║   Health check  http://localhost:${PORT}/health ║
║                                              ║
║   Files go P2P — this server only signals.  ║
╚══════════════════════════════════════════════╝
  `);
  serverLog('Server ready.');
});

// ─── GRACEFUL SHUTDOWN ────────────────────────────────────────────────────────
process.on('SIGTERM', () => {
  serverLog('SIGTERM received. Shutting down…');
  io.close(() => {
    server.close(() => process.exit(0));
  });
});
