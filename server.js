// DBAE online server — без зависимостей, нужен только Node.js 16+
// Запуск:  node server.js          (порт 8080, либо PORT=3000 node server.js)
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MAX_PLAYERS = 8;
const MAX_ROOMS = 200;
const MAX_MAP_BYTES = 4 * 1024 * 1024; // сжатая карта, base64
const MAX_FRAME = 16384;        // байт на сообщение
const RATE_PER_SEC = 60;       // сообщений в секунду от одного игрока
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const rooms = new Map();       // id -> room
const clients = new Set();
let nextClientId = 1;

// ---------- WebSocket (минимальная реализация RFC 6455) ----------
function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.from([0x80 | opcode, len]); }
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}

class Client {
  constructor(socket) {
    this.socket = socket;
    this.id = nextClientId++;
    this.name = 'Player';
    this.room = null;
    this.pid = 0;              // номер игрока внутри комнаты
    this.buf = Buffer.alloc(0);
    this.frag = null;
    this.alive = true;
    this.tokens = RATE_PER_SEC;
    this.lastState = null;
    this.helloDone = false;
  }
  send(obj) {
    if (!this.alive) return;
    try { this.socket.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (e) { this.close(); }
  }
  close() {
    if (!this.alive) return;
    this.alive = false;
    clients.delete(this);
    try { this.socket.write(frame(8, Buffer.alloc(0))); } catch (e) {}
    try { this.socket.destroy(); } catch (e) {}
    leaveRoom(this);
  }
  onData(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (!masked || len > MAX_FRAME) return this.close();
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4);
      const data = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 8) return this.close();
      if (op === 9) { try { this.socket.write(frame(10, data)); } catch (e) {} continue; }
      if (op === 10) { this.alive = true; this.pong = Date.now(); continue; }
      if (op === 1 || op === 2) { this.frag = fin ? null : [data]; if (fin) this.onMessage(data.toString('utf8')); }
      else if (op === 0 && this.frag) {
        this.frag.push(data);
        if (fin) { const all = Buffer.concat(this.frag); this.frag = null; if (all.length <= MAX_FRAME) this.onMessage(all.toString('utf8')); }
      }
    }
  }
  onMessage(text) {
    if (this.tokens < 1) return;      // слишком часто — игнорируем
    this.tokens--;
    let m;
    try { m = JSON.parse(text); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    handle(this, m);
  }
}

// ---------- логика ----------
function clean(s, max) {
  return String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, max);
}
function newRoomId() {
  for (let tries = 0; tries < 50; tries++) {
    let s = '';
    for (let i = 0; i < 5; i++) s += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!rooms.has(s)) return s;
  }
  return null;
}
function roomInfo(r) {
  return { i: r.id, n: r.name, p: r.players.size, m: r.max, l: r.pw ? 1 : 0, mp: r.mapReady ? 1 : 0, h: r.host };
}
function broadcast(room, obj, except) {
  const data = frame(1, Buffer.from(JSON.stringify(obj)));
  for (const c of room.players.values()) {
    if (c === except || !c.alive) continue;
    try { c.socket.write(data); } catch (e) {}
  }
}
function leaveRoom(c) {
  const r = c.room;
  if (!r) return;
  r.players.delete(c.pid);
  c.room = null;
  if (r.players.size === 0) rooms.delete(r.id);
  else {
    if (r.host === c.pid) { r.host = Math.min(...r.players.keys()); broadcast(r, { t: 'h', i: r.host }); }
    broadcast(r, { t: 'p-', i: c.pid });
  }
  c.lastState = null;
}
function joinRoom(c, r) {
  leaveRoom(c);
  let pid = 1;
  while (r.players.has(pid)) pid++;
  c.pid = pid; c.room = r;
  const others = [];
  for (const p of r.players.values()) others.push({ i: p.pid, n: p.name });
  r.players.set(pid, c);
  if (r.players.size === 1) r.host = pid;
  c.send({ t: 'joined', r: roomInfo(r), id: pid, p: others });
  broadcast(r, { t: 'p+', i: pid, n: c.name }, c);
  // положение машин, на которых уже ездили
  for (const e of r.vsnap.values()) for (const part of e.parts.values()) c.send(Object.assign({ t: 'vf' }, part));
  // новый игрок сразу получает последнее известное состояние остальных
  for (const p of r.players.values()) if (p !== c && p.lastState) c.send(Object.assign({ t: 's', i: p.pid }, p.lastState));
}

function handle(c, m) {
  switch (m.t) {
    case 'hi':
      c.name = clean(m.n, 16) || ('Player' + c.id);
      c.helloDone = true;
      c.send({ t: 'ok', id: c.id });
      break;
    case 'list': {
      const arr = [];
      for (const r of rooms.values()) if (!r.hidden) arr.push(roomInfo(r));
      c.send({ t: 'rooms', r: arr.slice(0, 50) });
      break;
    }
    case 'new': {
      if (!c.helloDone) return;
      if (rooms.size >= MAX_ROOMS) return c.send({ t: 'err', m: 'Сервер переполнен' });
      const id = newRoomId();
      if (!id) return c.send({ t: 'err', m: 'Не удалось создать комнату' });
      const r = {
        id, name: clean(m.n, 24) || (c.name + ' room'),
        pw: clean(m.pw, 16), hidden: !!m.pr,
        max: Math.max(2, Math.min(MAX_PLAYERS, parseInt(m.max, 10) || MAX_PLAYERS)),
        players: new Map(), created: Date.now(),
        host: 0, map: [], mapReady: false, mapVer: 0, mapBytes: 0, vsnap: new Map(), banned: new Map()
      };
      rooms.set(id, r);
      joinRoom(c, r);
      break;
    }
    case 'join': {
      if (!c.helloDone) return;
      const r = rooms.get(clean(m.c, 8).toUpperCase());
      if (!r) return c.send({ t: 'err', m: 'Комната не найдена' });
      if (r.players.size >= r.max) return c.send({ t: 'err', m: 'Комната заполнена' });
      if (r.pw && r.pw !== clean(m.pw, 16)) return c.send({ t: 'err', m: r.pw && !m.pw ? 'Нужен пароль' : 'Неверный пароль', pw: 1 });
      { const until = r.banned.get(c.name); if (until && until > Date.now()) return c.send({ t: 'err', m: 'Хост выгнал вас из комнаты' }); }
      joinRoom(c, r);
      break;
    }
    case 'mu': {       // загрузка карты хостом: куски gzip+base64 по порядку
      const r = c.room;
      if (!r || r.host !== c.pid) return;
      const k = m.k | 0, n = m.n | 0, d = typeof m.d === 'string' ? m.d : '';
      if (n < 1 || n > 400 || d.length > 14000) return c.send({ t: 'err', m: 'Карта слишком большая' });
      if (k === 0) { r.map = []; r.mapReady = false; r.mapBytes = 0; }
      if (k !== r.map.length) return c.send({ t: 'err', m: 'Ошибка загрузки карты' });
      r.mapBytes += d.length;
      if (r.mapBytes > MAX_MAP_BYTES) { r.map = []; return c.send({ t: 'err', m: 'Карта слишком большая (макс. 4 МБ)' }); }
      r.map.push(d);
      if (r.map.length === n) {
        r.mapReady = true; r.mapVer++;
        broadcast(r, { t: 'mr', v: r.mapVer, n });
      } else if (k % 10 === 0) c.send({ t: 'mp', k: r.map.length, n });
      break;
    }
    case 'mg': {       // скачать карту
      const r = c.room;
      if (!r) return;
      if (!r.mapReady) return c.send({ t: 'err', m: 'Хост ещё не выбрал карту' });
      const n = r.map.length;
      if (typeof m.k === 'number') {   // новый протокол: окно из 4 кусков начиная с k
        const k0 = Math.max(0, m.k | 0);
        for (let k = k0; k < Math.min(n, k0 + 4); k++) c.send({ t: 'md', k, n, d: r.map[k] });
      } else for (let k = 0; k < n; k++) c.send({ t: 'md', k, n, d: r.map[k] });
      break;
    }
    case 'leave': leaveRoom(c); c.send({ t: 'left' }); break;
    case 's': {        // состояние игрока (позиция и т.п.), храним последнее и рассылаем остальным
      if (!c.room) return;
      const st = {};
      for (const k of Object.keys(m)) if (k !== 't' && k !== 'i') st[k] = m[k];
      c.lastState = st;
      broadcast(c.room, Object.assign({ t: 's', i: c.pid }, st), c);
      break;
    }
    case 'e': {        // разовые события (эмоция, посадка в технику, выстрел...)
      if (!c.room) return;
      const ev = {};
      for (const k of Object.keys(m)) if (k !== 't' && k !== 'i') ev[k] = m[k];
      broadcast(c.room, Object.assign({ t: 'e', i: c.pid }, ev), c);
      break;
    }
    case 'ping': { c.send({ t: 'pong', x: m.x }); break; }
    case 'kick': {     // хост выгоняет игрока (блок по нику на 2 минуты)
      const r = c.room;
      if (!r || r.host !== c.pid) return;
      const t = r.players.get(m.i | 0);
      if (!t || t === c) return;
      r.banned.set(t.name, Date.now() + 120000);   // 2 минуты нельзя вернуться под тем же ником
      t.send({ t: 'kicked' });
      leaveRoom(t);
      break;
    }
    case 'host': {     // передача хоста
      const r = c.room;
      if (!r || r.host !== c.pid) return;
      const t = r.players.get(m.i | 0);
      if (!t) return;
      r.host = t.pid;
      broadcast(r, { t: 'h', i: r.host });
      break;
    }
    case 'vq': {       // клиент загрузил карту и просит положения машин
      const r = c.room;
      if (!r) return;
      for (const e of r.vsnap.values()) for (const part of e.parts.values()) c.send(Object.assign({ t: 'vf' }, part));
      break;
    }
    case 'vf': {       // снимок положения деталей машины после поездки: храним последний, рассылаем остальным
      const r = c.room;
      if (!r || typeof m.v !== 'string' || m.v.length > 64 || !Array.isArray(m.p) || m.p.length > 1100) return;
      const k = m.k | 0, cn = m.c | 0, o = m.o | 0;
      if (cn < 1 || cn > 64 || k < 0 || k >= cn) return;
      let e = r.vsnap.get(m.v);
      if (!e || e.c !== cn || k === 0) { if (!e && r.vsnap.size >= 64) return; e = { c: cn, parts: new Map() }; r.vsnap.set(m.v, e); }
      const part = { v: m.v, k, c: cn, o, p: m.p };
      e.parts.set(k, part);
      broadcast(r, Object.assign({ t: 'vf', i: c.pid }, part), c);
      break;
    }
    case 'a': {        // голос: короткий кусок аудио (base64), просто пересылаем остальным, не храним
      if (!c.room || typeof m.d !== 'string' || m.d.length > 2000) return;
      broadcast(c.room, { t: 'a', i: c.pid, d: m.d }, c);
      break;
    }
    case 'c': {
      if (!c.room) return;
      const text = clean(m.m, 120);
      if (text) broadcast(c.room, { t: 'c', i: c.pid, n: c.name, m: text });
      break;
    }
  }
}

// ---------- HTTP + upgrade ----------
const server = http.createServer((req, res) => {
  if (req.url === '/stats') {
    let players = 0; for (const r of rooms.values()) players += r.players.size;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const det = [];
    for (const r of rooms.values()) det.push({ id: r.id, name: r.name, players: r.players.size, host: r.host, mapReady: r.mapReady, chunks: r.map.length, bytes: r.mapBytes });
    return res.end(JSON.stringify({ rooms: rooms.size, players, detail: det }));
  }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('DBAE online server OK\n');
});
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = new Client(socket);
  c.pong = Date.now();
  clients.add(c);
  socket.on('data', d => { if (c.alive) c.onData(d); });
  socket.on('close', () => { c.alive = false; clients.delete(c); leaveRoom(c); });
  socket.on('end', () => c.close());
  socket.on('error', () => { c.alive = false; clients.delete(c); leaveRoom(c); });
});

setInterval(() => {        // проверяем живых и шлём ping
  const now = Date.now();
  for (const c of [...clients]) {
    if (now - c.pong > 45000) { c.close(); clients.delete(c); }
    else { try { c.socket.write(frame(9, Buffer.alloc(0))); } catch (e) {} }
  }
}, 15000).unref();
setInterval(() => { for (const c of clients) c.tokens = RATE_PER_SEC; }, 1000).unref();

server.listen(PORT, () => console.log('DBAE online server on :' + PORT));
