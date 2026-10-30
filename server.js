// שרת קטן: מגיש את המשחק ומעביר הודעות בין השחקנים באותו חדר.
// הוא לא מריץ את המשחק עצמו. המארח מריץ אותו והשרת רק מעביר.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'index.html');
const NAME_OK = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const MAX_PEERS = 24;
const MAX_PRESENCE_BYTES = 8192;

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health') { res.writeHead(200); res.end('ok'); return; }
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    fs.createReadStream(INDEX).pipe(res);
    return;
  }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });
const rooms = new Map(); // name -> Map(peerId -> ws)
let counter = 0;

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcast(room, from, obj) {
  const s = JSON.stringify(obj);
  for (const [id, ws] of room) if (id !== from && ws.readyState === 1) ws.send(s);
}
function merge(base, patch) {
  const out = Object.assign({}, base);
  for (const k of Object.keys(patch)) {
    if (patch[k] === null) delete out[k]; else out[k] = patch[k];
  }
  return out;
}

wss.on('connection', (ws) => {
  const id = 'p' + (++counter).toString(36) + Math.random().toString(36).slice(2, 6);
  ws.id = id; ws.alive = true; ws.room = null; ws.presence = {}; ws.count = 0;
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', (data) => {
    if (++ws.count > 120) return; // reset every second
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'join' && !ws.room) {
      if (typeof m.name !== 'string' || !NAME_OK.test(m.name)) { send(ws, { t: 'error', code: 'bad_name' }); return; }
      let room = rooms.get(m.name);
      if (!room) { room = new Map(); rooms.set(m.name, room); }
      if (room.size >= MAX_PEERS) { send(ws, { t: 'error', code: 'full' }); return; }
      ws.room = m.name;
      const others = [...room.entries()].map(([pid, w]) => ({ peer: pid, presence: w.presence }));
      room.set(id, ws);
      send(ws, { t: 'welcome', peer: id, peers: others });
      broadcast(room, id, { t: 'pres', peer: id, patch: {} });
    } else if (m.t === 'pres' && ws.room) {
      const room = rooms.get(ws.room);
      if (!room || !m.patch || typeof m.patch !== 'object' || Array.isArray(m.patch)) return;
      const next = merge(ws.presence, m.patch);
      if (JSON.stringify(next).length > MAX_PRESENCE_BYTES) return;
      ws.presence = next;
      broadcast(room, id, { t: 'pres', peer: id, patch: m.patch });
    }
  });

  ws.on('close', () => {
    if (!ws.room) return;
    const room = rooms.get(ws.room);
    if (!room) return;
    room.delete(id);
    if (room.size === 0) rooms.delete(ws.room);
    else broadcast(room, id, { t: 'left', peer: id });
  });
  ws.on('error', () => {});
});

setInterval(() => { for (const ws of wss.clients) ws.count = 0; }, 1000);
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log('ze1 listening on ' + PORT));
