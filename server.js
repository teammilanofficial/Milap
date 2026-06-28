const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' }, transports: ['websocket', 'polling'] });

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/milap';
const MASTER_CODE = process.env.MASTER_CODE || 'MILAP-FIRST';
const JWT_SECRET = process.env.JWT_SECRET || 'change-me-in-render-env';

mongoose.connect(MONGODB_URI).then(() => console.log('MongoDB OK')).catch(e => console.error(e));

const userSchema = new mongoose.Schema({
  email: { type: String, unique: true, required: true, lowercase: true, trim: true },
  password: { type: String, required: true },
  username: { type: String, required: true, trim: true },
  inviteCode: { type: String, unique: true, default: () => 'MILAP-' + Math.random().toString(36).slice(2, 7).toUpperCase() },
  contacts: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }]
});
const User = mongoose.model('User', userSchema);

const msgSchema = new mongoose.Schema({
  from: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  to: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  text: { type: String, required: true },
  read: { type: Boolean, default: false },
  timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', msgSchema);

function auth(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); } catch { res.status(401).json({ error: 'Invalid token' }); }
}

app.post('/register', async (req, res) => {
  try {
    const { email, password, username, inviteCode } = req.body;
    if (!email || !password || !username || !inviteCode) return res.status(400).json({ error: 'All fields required' });
    const existing = await User.findOne({ email: email.toLowerCase().trim() });
    if (existing) return res.status(400).json({ error: 'Email already registered' });
    if (password.length < 6) return res.status(400).json({ error: 'Password too short' });
    
    let inviter = null;
    if (inviteCode !== MASTER_CODE) {
      inviter = await User.findOne({ inviteCode: inviteCode.trim().toUpperCase() });
      if (!inviter) return res.status(400).json({ error: 'Invalid invite code' });
    }
    
    const hash = await bcrypt.hash(password, 10);
    const user = new User({ email: email.toLowerCase().trim(), password: hash, username: username.trim() });
    if (inviter) user.contacts = [inviter._id];
    await user.save();
    
    if (inviter) {
      inviter.contacts.push(user._id);
      await inviter.save();
    }
    
    const token = jwt.sign({ id: user._id, username: user.username }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { email: user.email, username: user.username, inviteCode: user.inviteCode } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await User.findOne({ email: email.toLowerCase().trim() });
    if (!user || !await bcrypt.compare(password, user.password)) return res.status(400).json({ error: 'Invalid credentials' });
    const token = jwt.sign({ id: user._id, username: user.username }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { email: user.email, username: user.username, inviteCode: user.inviteCode } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/me', auth, async (req, res) => {
  try {
    const user = await User.findById(req.user.id).populate('contacts', 'username email inviteCode');
    res.json(user);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/search', auth, async (req, res) => {
  try {
    const q = req.query.q?.trim();
    if (!q) return res.json([]);
    const users = await User.find({ 
      $or: [{ email: q.toLowerCase() }, { username: { $regex: q, $options: 'i' } }],
      _id: { $ne: req.user.id }
    }).select('username email inviteCode');
    res.json(users);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/add-contact', auth, async (req, res) => {
  try {
    const me = await User.findById(req.user.id);
    if (!me.contacts.includes(req.body.userId)) {
      me.contacts.push(req.body.userId);
      await me.save();
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/messages/:contactId', auth, async (req, res) => {
  try {
    const msgs = await Message.find({
      $or: [
        { from: req.user.id, to: req.params.contactId },
        { from: req.params.contactId, to: req.user.id }
      ]
    }).sort({ timestamp: 1 }).limit(200).lean();
    res.json(msgs);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/', (req, res) => res.json({ name: 'Milap', status: 'running' }));

io.use((socket, next) => {
  const token = socket.handshake.auth?.token;
  if (!token) return next(new Error('No token'));
  try { socket.user = jwt.verify(token, JWT_SECRET); next(); } catch { next(new Error('Bad token')); }
});

io.on('connection', (socket) => {
  const uid = socket.user.id;
  socket.join(uid);
  
  socket.on('message', async ({ to, text }) => {
    if (!to || !text?.trim()) return;
    try {
      const msg = new Message({ from: uid, to, text: text.trim().substring(0, 1000) });
      await msg.save();
      const payload = { _id: msg._id, from: { _id: uid, username: socket.user.username }, to, text: msg.text, timestamp: msg.timestamp };
      io.to(to).emit('message', payload);
      io.to(uid).emit('message', payload);
    } catch (e) { console.error(e); }
  });
  
  socket.on('disconnect', () => {});
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on ${PORT}`));
