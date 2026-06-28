const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling']
});

/* ================= MongoDB ================= */
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/milap';

mongoose.connect(MONGODB_URI)
  .then(() => console.log('✅ MongoDB connected'))
  .catch(err => console.error('❌ MongoDB error:', err));

const messageSchema = new mongoose.Schema({
  room: { type: String, required: true, index: true },
  username: { type: String, required: true },
  text: { type: String, required: true },
  timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', messageSchema);

/* ================= State ================= */
const users = new Map(); // socket.id -> { username, room }

function getRoomUsers(room) {
  const list = [];
  users.forEach((u) => { if (u.room === room) list.push(u.username); });
  return [...new Set(list)];
}

/* ================= Socket.IO ================= */
io.on('connection', (socket) => {
  console.log('🔌 Client connected:', socket.id);

  socket.on('join', async ({ username, room }) => {
    if (!username || !room) return;
    const cleanName = username.trim().substring(0, 30);
    users.set(socket.id, { username: cleanName, room });
    socket.join(room);

    try {
      const history = await Message.find({ room }).sort({ timestamp: 1 }).limit(200).lean();
      socket.emit('history', history);
    } catch (e) {
      console.error('History error:', e);
    }

    socket.to(room).emit('notification', {
      type: 'join',
      text: `${cleanName} joined`,
      users: getRoomUsers(room)
    });
    socket.emit('users', getRoomUsers(room));
  });

  socket.on('message', async ({ text, room }) => {
    const user = users.get(socket.id);
    if (!user || !text || !room) return;
    const cleanText = text.trim().substring(0, 1000);
    if (!cleanText) return;

    try {
      const msg = new Message({ room, username: user.username, text: cleanText });
      await msg.save();
      io.to(room).emit('message', {
        username: user.username,
        text: msg.text,
        timestamp: msg.timestamp
      });
    } catch (e) {
      console.error('Message save error:', e);
    }
  });

  socket.on('typing', ({ room, isTyping }) => {
    const user = users.get(socket.id);
    if (!user) return;
    socket.to(room).emit('typing', { username: user.username, isTyping });
  });

  socket.on('disconnect', () => {
    const user = users.get(socket.id);
    if (user) {
      users.delete(socket.id);
      socket.to(user.room).emit('notification', {
        type: 'leave',
        text: `${user.username} left`,
        users: getRoomUsers(user.room)
      });
    }
    console.log('🔌 Client disconnected:', socket.id);
  });
});

/* ================= Health Check ================= */
app.get('/', (req, res) => {
  res.json({ name: 'Milap Server', status: 'running', time: new Date() });
});
app.get('/health', (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Milap server running on port ${PORT}`));
