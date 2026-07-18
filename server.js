const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const stations = new Map();

// Cache file read for better performance
let indexHtmlCache = null;
fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
    if (!err) indexHtmlCache = data;
});

const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/index.html') {
        if (indexHtmlCache) {
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(indexHtmlCache);
        } else {
            fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
                if (err) { res.writeHead(500); res.end('Hata'); return; }
                indexHtmlCache = data;
                res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
                res.end(data);
            });
        }
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    ws.alive = true;
    ws.station = null;
    ws.nick = null;
    ws.role = null;
    ws.blocked = false;

    ws.on('message', (raw, isBinary) => {
        if (ws.blocked) return;
        if (isBinary) {
            const st = stations.get(ws.station);
            if (st) {
                const binaryMsg = Buffer.from(raw);
                st.listeners.forEach(l => {
                    if (!l.blocked && l.readyState === WebSocket.OPEN) l.send(binaryMsg);
                });
            }
            return;
        }
        let msg;
        try { msg = JSON.parse(raw); } catch (e) { return; }

        switch (msg.type) {
            case 'join':
                ws.station = msg.station;
                ws.nick = msg.nick;
                ws.role = msg.role;
                if (!stations.has(ws.station)) {
                    stations.set(ws.station, {
                        bc: null, song: '', listeners: new Set(),
                        requests: [], blocked: new Set(),
                        schedule: [],
                        stats: { peak: 0, totalJoined: 0 }
                    });
                }
                const st = stations.get(ws.station);
                if (st.blocked.has(ws.nick.toLowerCase())) {
                    ws.blocked = true;
                    ws.send(JSON.stringify({ type: 'blocked' }));
                    ws.close();
                    return;
                }
                st.stats.totalJoined++;
                if (msg.role === 'broadcaster') {
                    st.bc = ws;
                    ws.send(JSON.stringify({ type: 'joined', role: 'broadcaster' }));
                } else {
                    st.listeners.add(ws);
                    const count = st.listeners.size + (st.bc ? 1 : 0);
                    if (count > st.stats.peak) st.stats.peak = count;
                    ws.send(JSON.stringify({
                        type: 'joined', role: 'listener',
                        active: !!st.bc, song: st.song,
                        count: count, sampleRate: msg.sampleRate || 44100,
                        schedule: st.schedule || []
                    }));
                    if (st.bc && st.bc.readyState === WebSocket.OPEN) {
                        st.bc.send(JSON.stringify({ type: 'listener-update', count: count, stats: st.stats }));
                    }
                }
                break;

            case 'chat':
                if (!ws.station) return;
                sendAll(ws.station, { type: 'chat', nick: ws.nick, text: msg.text, time: Date.now() });
                break;

            case 'request':
                if (!ws.station || ws.role !== 'listener') return;
                const stR = stations.get(ws.station);
                if (stR) {
                    const req = { id: Date.now() + Math.random(), nick: ws.nick, song: msg.song, time: Date.now() };
                    stR.requests.push(req);
                    if (stR.bc && stR.bc.readyState === WebSocket.OPEN) {
                        stR.bc.send(JSON.stringify({ type: 'new-request', request: req, queue: stR.requests }));
                    }
                    ws.send(JSON.stringify({ type: 'request-sent' }));
                }
                break;

            case 'request-action':
                if (!ws.station || ws.role !== 'broadcaster') return;
                const stA = stations.get(ws.station);
                if (stA) {
                    let removed = null;
                    stA.requests = stA.requests.filter(r => {
                        if (r.id === msg.requestId) { removed = r; return false; }
                        return true;
                    });
                    if (msg.accepted && removed) {
                        stA.listeners.forEach(l => {
                            if (l.readyState === WebSocket.OPEN && l.nick === removed.nick) {
                                l.send(JSON.stringify({ type: 'request-accepted', song: removed.song }));
                            }
                        });
                    }
                    ws.send(JSON.stringify({ type: 'queue-update', queue: stA.requests }));
                }
                break;

            case 'block-listener':
                if (!ws.station || ws.role !== 'broadcaster') return;
                const stB = stations.get(ws.station);
                if (stB) {
                    stB.blocked.add(msg.nick.toLowerCase());
                    stB.listeners.forEach(l => {
                        if (l.nick && l.nick.toLowerCase() === msg.nick.toLowerCase()) {
                            l.blocked = true;
                            l.send(JSON.stringify({ type: 'blocked' }));
                            setTimeout(() => { try { l.close(); } catch(e) {} }, 500);
                        }
                    });
                    sendAll(ws.station, { type: 'system', text: msg.nick + ' yayindan atildi' }, ws);
                }
                break;

            case 'broadcast-start':
                if (!ws.station) return;
                const s2 = stations.get(ws.station);
                if (s2) {
                    s2.song = msg.song || '';
                    if (msg.schedule) s2.schedule = msg.schedule;
                    sendAll(ws.station, {
                        type: 'broadcast-start', nick: ws.nick, song: s2.song,
                        sampleRate: msg.sampleRate || 44100, schedule: s2.schedule || []
                    }, ws);
                }
                break;

            case 'broadcast-stop':
                if (!ws.station) return;
                const s3 = stations.get(ws.station);
                if (s3) {
                    s3.bc = null; s3.song = '';
                    sendAll(ws.station, { type: 'broadcast-stop' });
                    s3.listeners.forEach(l => { try { l.close(); } catch(e) {} });
                    s3.listeners.clear();
                    stations.delete(ws.station);
                }
                break;

            case 'song-update':
                if (!ws.station) return;
                const s4 = stations.get(ws.station);
                if (s4) { s4.song = msg.song || ''; sendAll(ws.station, { type: 'song-update', song: s4.song }, ws); }
                break;

            case 'update-schedule':
                if (!ws.station || ws.role !== 'broadcaster') return;
                const s5 = stations.get(ws.station);
                if (s5) { s5.schedule = msg.schedule || []; sendAll(ws.station, { type: 'schedule-update', schedule: s5.schedule }, ws); }
                break;
        }
    });

    ws.on('close', () => {
        if (!ws.station || !stations.has(ws.station)) return;
        const st = stations.get(ws.station);
        if (ws.role === 'broadcaster') {
            st.bc = null; st.song = '';
            sendAll(ws.station, { type: 'broadcast-stop' });
            st.listeners.forEach(l => { try { l.close(); } catch(e) {} });
            st.listeners.clear();
            stations.delete(ws.station);
        } else if (ws.role === 'listener' && !ws.blocked) {
            st.listeners.delete(ws);
            const cnt = st.listeners.size + (st.bc ? 1 : 0);
            if (st.bc && st.bc.readyState === WebSocket.OPEN) {
                st.bc.send(JSON.stringify({ type: 'listener-update', count: cnt, stats: st.stats }));
            }
        }
    });

    ws.on('pong', () => { ws.alive = true; });
});

setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws.alive) return ws.terminate();
        ws.alive = false;
        ws.ping();
    });
}, 30000);

function sendAll(station, msg, exclude) {
    const st = stations.get(station);
    if (!st) return;
    const data = JSON.stringify(msg);
    if (st.bc && st.bc !== exclude && st.bc.readyState === WebSocket.OPEN) st.bc.send(data);
    st.listeners.forEach(l => {
        if (l !== exclude && !l.blocked && l.readyState === WebSocket.OPEN) l.send(data);
    });
}

server.listen(PORT, function() {
    console.log('Radio Studio v2 sunucu hazir - port ' + PORT);
});