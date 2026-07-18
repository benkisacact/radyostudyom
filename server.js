const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
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
    ws.blocked = false;

    ws.on('message', (raw, isBinary) => {
        if (ws.blocked) return;
        if (isBinary) {
            var st = stations.get(ws.station);
            if (st) {
                st.listeners.forEach(function(l) {
                    if (!l.blocked && l.readyState === 1) l.send(raw, { binary: true });
                });
            }
            return;
        }
        var msg;
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
                var st = stations.get(ws.station);
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
                    var count = st.listeners.size + (st.bc ? 1 : 0);
                    if (count > st.stats.peak) st.stats.peak = count;
                    ws.send(JSON.stringify({
                        type: 'joined', role: 'listener',
                        active: !!st.bc, song: st.song,
                        count: count, sampleRate: msg.sampleRate || 44100,
                        schedule: st.schedule || []
                    }));
                    if (st.bc && st.bc.readyState === 1) {
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
                var stR = stations.get(ws.station);
                if (stR) {
                    var req = { id: Date.now() + Math.random(), nick: ws.nick, song: msg.song, time: Date.now() };
                    stR.requests.push(req);
                    if (stR.bc && stR.bc.readyState === 1) {
                        stR.bc.send(JSON.stringify({ type: 'new-request', request: req, queue: stR.requests }));
                    }
                    ws.send(JSON.stringify({ type: 'request-sent' }));
                }
                break;

            case 'request-action':
                if (!ws.station || ws.role !== 'broadcaster') return;
                var stA = stations.get(ws.station);
                if (stA) {
                    var removed = null;
                    stA.requests = stA.requests.filter(function(r) {
                        if (r.id === msg.requestId) { removed = r; return false; }
                        return true;
                    });
                    if (msg.accepted && removed) {
                        stA.listeners.forEach(function(l) {
                            if (l.readyState === 1 && l.nick === removed.nick) {
                                l.send(JSON.stringify({ type: 'request-accepted', song: removed.song }));
                            }
                        });
                    }
                    ws.send(JSON.stringify({ type: 'queue-update', queue: stA.requests }));
                }
                break;

            case 'block-listener':
                if (!ws.station || ws.role !== 'broadcaster') return;
                var stB = stations.get(ws.station);
                if (stB) {
                    stB.blocked.add(msg.nick.toLowerCase());
                    stB.listeners.forEach(function(l) {
                        if (l.nick && l.nick.toLowerCase() === msg.nick.toLowerCase()) {
                            l.blocked = true;
                            l.send(JSON.stringify({ type: 'blocked' }));
                            setTimeout(function() { try { l.close(); } catch(e) {} }, 500);
                        }
                    });
                    sendAll(ws.station, { type: 'system', text: msg.nick + ' yayindan atildi' }, ws);
                }
                break;

            case 'broadcast-start':
                if (!ws.station) return;
                var s2 = stations.get(ws.station);
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
                var s3 = stations.get(ws.station);
                if (s3) {
                    s3.bc = null; s3.song = '';
                    sendAll(ws.station, { type: 'broadcast-stop' });
                    s3.listeners.forEach(function(l) { try { l.close(); } catch(e) {} });
                    s3.listeners.clear();
                    stations.delete(ws.station);
                }
                break;

            case 'song-update':
                if (!ws.station) return;
                var s4 = stations.get(ws.station);
                if (s4) { s4.song = msg.song || ''; sendAll(ws.station, { type: 'song-update', song: s4.song }, ws); }
                break;

            case 'update-schedule':
                if (!ws.station || ws.role !== 'broadcaster') return;
                var s5 = stations.get(ws.station);
                if (s5) { s5.schedule = msg.schedule || []; sendAll(ws.station, { type: 'schedule-update', schedule: s5.schedule }, ws); }
                break;
        }
    });

    ws.on('close', function() {
        if (!ws.station || !stations.has(ws.station)) return;
        var st = stations.get(ws.station);
        if (ws.role === 'broadcaster') {
            st.bc = null; st.song = '';
            sendAll(ws.station, { type: 'broadcast-stop' });
            st.listeners.forEach(function(l) { try { l.close(); } catch(e) {} });
            st.listeners.clear();
            stations.delete(ws.station);
        } else if (ws.role === 'listener' && !ws.blocked) {
            st.listeners.delete(ws);
            var cnt = st.listeners.size + (st.bc ? 1 : 0);
            if (st.bc && st.bc.readyState === 1) {
                st.bc.send(JSON.stringify({ type: 'listener-update', count: cnt, stats: st.stats }));
            }
        }
    });

    ws.on('pong', function() { ws.alive = true; });
});

setInterval(function() {
    wss.clients.forEach(function(ws) {
        if (!ws.alive) return ws.terminate();
        ws.alive = false;
        ws.ping();
    });
}, 30000);

function sendAll(station, msg, exclude) {
    var st = stations.get(station);
    if (!st) return;
    var data = JSON.stringify(msg);
    if (st.bc && st.bc !== exclude && st.bc.readyState === 1) st.bc.send(data);
    st.listeners.forEach(function(l) {
        if (l !== exclude && !l.blocked && l.readyState === 1) l.send(data);
    });
}

server.listen(PORT, function() {
    console.log('Radio Studio v2 sunucu hazir - port ' + PORT);
});