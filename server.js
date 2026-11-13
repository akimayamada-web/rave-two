const http = require("http");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const rooms = new Map();

const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, `http://${req.headers.host}`).pathname;
  const file = pathname === "/" ? "index.html" : pathname.slice(1);
  const full = path.join(ROOT, file);
  if (!full.startsWith(ROOT) || !fs.existsSync(full) || fs.statSync(full).isDirectory()) {
    res.writeHead(404); return res.end("Not found");
  }
  const types = { ".html": "text/html; charset=utf-8", ".css": "text/css", ".js": "text/javascript" };
  res.writeHead(200, { "Content-Type": types[path.extname(full)] || "application/octet-stream" });
  fs.createReadStream(full).pipe(res);
});

const wss = new WebSocket.Server({ server });

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function broadcast(room, data, except) {
  for (const client of room.clients) if (client !== except) send(client, data);
}
function cleanRoom(code) {
  const room = rooms.get(code);
  if (room && room.clients.size === 0) rooms.delete(code);
}

wss.on("connection", ws => {
  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === "join") {
      const code = String(msg.code || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 8);
      if (!code) return send(ws, { type: "error", message: "Введите код комнаты." });
      let room = rooms.get(code);
      if (!room) {
        room = { clients: new Set(), videoUrl: "", playing: false, time: 0, updatedAt: Date.now() };
        rooms.set(code, room);
      }
      if (room.clients.size >= 2) return send(ws, { type: "error", message: "В комнате уже два человека." });
      room.clients.add(ws);
      ws.roomCode = code;
      send(ws, { type: "joined", code, count: room.clients.size, videoUrl: room.videoUrl, playing: room.playing, time: room.time });
      broadcast(room, { type: "presence", count: room.clients.size }, ws);
      return;
    }

    const code = ws.roomCode;
    const room = code && rooms.get(code);
    if (!room) return;

    if (msg.type === "video") {
      try {
        const u = new URL(msg.url);
        if (!["http:", "https:"].includes(u.protocol)) throw 0;
        const host = u.hostname.toLowerCase().replace(/^www\\./, "");
        const isYouTube = ["youtube.com", "m.youtube.com", "youtu.be", "youtube-nocookie.com"].includes(host) || host.endsWith(".youtube.com");
        const isVideoFile = /\\.(mp4|webm|ogg)(?:$)/i.test(u.pathname);
        if (!isYouTube && !isVideoFile) throw 0;
      } catch {
        return send(ws, { type: "error", message: "Поддерживаются ссылки YouTube или прямые MP4/WebM." });
      }
      room.videoUrl = msg.url;
      room.playing = false;
      room.time = 0;
      room.updatedAt = Date.now();
      broadcast(room, { type: "video", url: room.videoUrl, playing: false, time: 0 });
    } else if (msg.type === "sync") {
      room.playing = !!msg.playing;
      room.time = Math.max(0, Number(msg.time) || 0);
      room.updatedAt = Date.now();
      broadcast(room, { type: "sync", playing: room.playing, time: room.time, sentAt: room.updatedAt }, ws);
    }
  });

  ws.on("close", () => {
    const code = ws.roomCode;
    const room = code && rooms.get(code);
    if (!room) return;
    room.clients.delete(ws);
    broadcast(room, { type: "presence", count: room.clients.size });
    cleanRoom(code);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Rave Two prototype running at http://localhost:${PORT}`);
});