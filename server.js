const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// Radyo istasyonlari: name -> { bc: ws|null, song, listeners: Set<ws> }
const stations = new Map();

const server = http.createServer((req, res) => {
    if (req.url === '/' || req.url === '/index.html') {
        fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
            if (err) { res.writeHead(500); res.end('Hata'); return; }
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(data);
        });
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

    ws.on('message', (raw, isBinary) => {
        if (isBinary) {
            // Ses verisi: yayincidan tum dinleyicilere yonlendir
            const st = stations.get(ws.station);
            if (st) {
                st.listeners.forEach(l => {
                    if (l.readyState === 1) l.send(raw, { binary: true });
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
                    stations.set(ws.station, { bc: null, song: '', listeners: new Set() });
                }
                const st = stations.get(ws.station);
                if (msg.role === 'broadcaster') {
                    st.bc = ws;
                    ws.send(JSON.stringify({
                        type: 'joined', role: 'broadcaster'
                    }));
                } else {
                    st.listeners.add(ws);
                    ws.send(JSON.stringify({
                        type: 'joined', role: 'listener',
                        active: !!st.bc, song: st.song,
                        count: st.listeners.size + (st.bc ? 1 : 0),
                        sampleRate: msg.sampleRate || 44100
                    }));
                    if (st.bc && st.bc.readyState === 1) {
                        st.bc.send(JSON.stringify({
                            type: 'listener-update', count: st.listeners.size + 1
                        }));
                    }
                }
                break;

            case 'chat':
                if (!ws.station) return;
                sendAll(ws.station, {
                    type: 'chat', nick: ws.nick,
                    text: msg.text, time: Date.now()
                });
                break;

            case 'broadcast-start':
                if (!ws.station) return;
                var s2 = stations.get(ws.station);
                if (s2) {
                    s2.song = msg.song || '';
                    sendAll(ws.station, {
                        type: 'broadcast-start',
                        nick: ws.nick, song: s2.song,
                        sampleRate: msg.sampleRate || 44100
                    }, ws);
                }
                break;

            case 'broadcast-stop':
                if (!ws.station) return;
                var s3 = stations.get(ws.station);
                if (s3) {
                    s3.bc = null;
                    s3.song = '';
                    sendAll(ws.station, { type: 'broadcast-stop' });
                    s3.listeners.clear();
                    stations.delete(ws.station);
                }
                break;

            case 'song-update':
                if (!ws.station) return;
                var s4 = stations.get(ws.station);
                if (s4) {
                    s4.song = msg.song || '';
                    sendAll(ws.station, { type: 'song-update', song: s4.song }, ws);
                }
                break;
        }
    });

    ws.on('close', () => {
        if (!ws.station || !stations.has(ws.station)) return;
        var st = stations.get(ws.station);
        if (ws.role === 'broadcaster') {
            st.bc = null;
            st.song = '';
            sendAll(ws.station, { type: 'broadcast-stop' });
            st.listeners.forEach(l => {
                try { l.close(); } catch (e) {}
            });
            st.listeners.clear();
            stations.delete(ws.station);
        } else if (ws.role === 'listener') {
            st.listeners.delete(ws);
            var cnt = st.listeners.size + (st.bc ? 1 : 0);
            sendAll(ws.station, {
                type: 'listener-update', count: cnt
            });
            if (st.bc && st.bc.readyState === 1) {
                st.bc.send(JSON.stringify({
                    type: 'listener-update', count: cnt
                }));
            }
        }
    });

    ws.on('pong', () => { ws.alive = true; });
});

// 30 saniyede bir heartbeat
setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws.alive) return ws.terminate();
        ws.alive = false;
        ws.ping();
    });
}, 30000);

function sendAll(station, msg, exclude) {
    var st = stations.get(station);
    if (!st) return;
    var data = JSON.stringify(msg);
    if (st.bc && st.bc !== exclude && st.bc.readyState === 1) {
        st.bc.send(data);
    }
    st.listeners.forEach(l => {
        if (l !== exclude && l.readyState === 1) l.send(data);
    });
}

server.listen(PORT, () => {
    console.log('Radio Studio sunucu hazir - port ' + PORT);
});