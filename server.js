// Serveur du quiz en direct : sert les fichiers du dossier "public"
// et relaie les messages entre l'écran de l'animateur et les smartphones.
const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const PUBLIC = __dirname;
const TYPES = { ".html": "text/html; charset=utf-8", ".json": "application/json; charset=utf-8",
  ".js": "text/javascript", ".css": "text/css", ".png": "image/png", ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg", ".svg": "image/svg+xml", ".webp": "image/webp", ".gif": "image/gif" };

const server = http.createServer((req, res) => {
  let p = decodeURIComponent(new URL(req.url, "http://x").pathname);
  if (p === "/health") { res.writeHead(200); return res.end("ok"); }
  if (p.endsWith("/")) p += "index.html";
  const file = path.normalize(path.join(PUBLIC, p));
  if (!file.startsWith(PUBLIC) || /node_modules|\.git/.test(file)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end("Introuvable"); }
    res.writeHead(200, { "Content-Type": TYPES[path.extname(file).toLowerCase()] || "application/octet-stream",
      "Cache-Control": "no-cache" });
    res.end(data);
  });
});

// rooms : code -> { host: socket|null, players: Map(pid -> socket), hostLeftAt }
const rooms = new Map();
const ALPHA = "0123456789";
const newCode = () => { let c; do { c = Array.from({ length: 4 }, () => ALPHA[Math.floor(Math.random() * ALPHA.length)]).join(""); } while (rooms.has(c)); return c; };
let nextPid = 1;
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 64 * 1024 });
wss.on("connection", ws => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.t === "host") {
      // reprise d'une salle existante (après une coupure) ou création
      let code = typeof m.code === "string" && rooms.has(m.code) ? m.code : newCode();
      let room = rooms.get(code);
      if (!room) { room = { host: null, players: new Map(), hostLeftAt: 0 }; rooms.set(code, room); }
      if (room.host && room.host !== ws) { try { room.host.close(); } catch {} }
      room.host = ws; room.hostLeftAt = 0;
      ws.role = "host"; ws.code = code;
      send(ws, { t: "hosted", code, pids: [...room.players.keys()] });
    }
    else if (m.t === "join") {
      const code = String(m.code || "").trim().toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { t: "noroom" });
      const pid = "p" + (nextPid++);
      ws.role = "player"; ws.code = code; ws.pid = pid;
      room.players.set(pid, ws);
      send(ws, { t: "joined" });
      send(room.host, { t: "conn", pid });
    }
    else if (m.t === "msg" && ws.role === "player") {
      const room = rooms.get(ws.code);
      if (room) send(room.host, { t: "msg", pid: ws.pid, data: m.data });
    }
    else if ((m.t === "to" || m.t === "kick") && ws.role === "host") {
      const room = rooms.get(ws.code);
      const p = room && room.players.get(m.pid);
      if (!p) return;
      if (m.t === "to") send(p, { t: "msg", data: m.data });
      else { room.players.delete(m.pid); setTimeout(() => { try { p.close(); } catch {} }, 300); }
    }
  });
  ws.on("close", () => {
    const room = rooms.get(ws.code);
    if (!room) return;
    if (ws.role === "host" && room.host === ws) { room.host = null; room.hostLeftAt = Date.now(); }
    if (ws.role === "player" && room.players.get(ws.pid) === ws) {
      room.players.delete(ws.pid);
      send(room.host, { t: "close", pid: ws.pid });
    }
  });
});

// battements de cœur (évite les coupures des connexions inactives) + ménage des salles abandonnées
setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; try { ws.ping(); } catch {} });
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (!room.host && room.hostLeftAt && now - room.hostLeftAt > 30 * 60 * 1000) {
      room.players.forEach(p => { try { p.close(); } catch {} });
      rooms.delete(code);
    }
  }
}, 25000);

server.listen(PORT, () => console.log("Quiz en direct sur le port " + PORT));
