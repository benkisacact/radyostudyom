require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const BROADCAST_PIN = process.env.BROADCAST_PIN || "0000";
const stations = new Map();

// GLM API Ayarları
const GLM_API_URL = process.env.GLM_API_URL || "https://open.bigmodel.cn/api/paas/v4/chat/completions";
const GLM_API_KEY = process.env.GLM_API_KEY || "SENIN_GLM_API_ANAHTARIN";
const AI_NICK = "GLM-DJ";

async function getAIResponse(prompt, currentSong) {
    if (!GLM_API_KEY || GLM_API_KEY === "SENIN_GLM_API_ANAHTARIN") return "Yapay zeka API anahtarı ayarlanmamış.";
    const systemPrompt = `Sen "${AI_NICK}" adında bir radyo istasyonunun yapay zeka sunucususun. Şu an radyoda çalan şarkı: ${currentSong || 'Bilinmiyor'}. Cevapların kısa ve eğlenceli olsun.`;
    try {
        const response = await fetch(GLM_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GLM_API_KEY}` },
            body: JSON.stringify({
                model: "glm-4",
                messages: [
                    { role: "system", content: systemPrompt },
                    { role: "user", content: prompt }
                ],
                temperature: 0.7,
                max_tokens: 150
            })
        });
        const data = await response.json();
        return data.choices && data.choices.length > 0 ? data.choices[0].message.content.trim() : "Şu an cevap veremiyorum.";
    } catch (error) {
        console.error("GLM Hatası:", error);
        return "Bağlantıda aksaklık var.";
    }
}

const server = http.createServer((req, res) => {
    if (req.url === '/worklet-processor.js') {
        fs.readFile(path.join(__dirname, 'worklet-processor.js'), (err, data) => {
            if (err) { res.writeHead(500); res.end('Hata'); return; }
            res.writeHead(200, { 'Content-Type': 'application/javascript; charset=utf-8' });
            res.end(data);
        });
    } else if (req.url === '/' || req.url === '/index.html') {
        fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
            if (err) { res.writeHead(500); res.end('Hata'); return; }
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(data);
        });
    } else {
        res.writeHead(404); res.end();
    }
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    ws.alive = true;
    ws.station = null;
    ws.nick = null;
    ws.role = null;
    ws.blocked = false;

    ws.on('message', async (raw, isBinary) => {
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

                if (msg.role === 'broadcaster' && msg.pin !== BROADCAST_PIN) {
                    ws.send(JSON.stringify({ type: 'error', message: 'Hatalı Yayıncı PIN!' }));
                    ws.close();
                    return;
                }

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
                    var initCount = st.listeners.size + 1;
                    if (initCount > st.stats.peak) st.stats.peak = initCount;
                    ws.send(JSON.stringify({ type: 'listener-update', count: initCount, stats: st.stats }));
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
                        st.bc.send(JSON.stringify({
                            type: 'listener-update',
                            count: count,
                            stats: st.stats
                        }));
                    }
                }
                break;

            case 'chat':
                if (!ws.station) return;
                if (msg.text && msg.text.toLowerCase().startsWith('!ai ')) {
                    var aiPrompt = msg.text.substring(4).trim();
                    var currentSong = stations.get(ws.station) ? stations.get(ws.station).song : '';
                    ws.send(JSON.stringify({ type: 'chat', nick: AI_NICK, text: "Düşünüyorum...", time: Date.now() }));
                    var aiReply = await getAIResponse(aiPrompt, currentSong);
                    sendAll(ws.station, { type: 'chat', nick: AI_NICK, text: aiReply, time: Date.now() });
                    return;
                }
                sendAll(ws.station, {
                    type: 'chat', nick: ws.nick,
                    text: msg.text, time: Date.now()
                });
                break;

            case 'request':
                if (!ws.station || ws.role !== 'listener') return;
                var stReq = stations.get(ws.station);
                if (stReq) {
                    var req = {
                        id: Date.now() + Math.random(),
                        nick: ws.nick,
                        song: msg.song,
                        time: Date.now()
                    };
                    stReq.requests.push(req);
                    if (stReq.bc && stReq.bc.readyState === 1) {
                        stReq.bc.send(JSON.stringify({
                            type: 'new-request',
                            request: req,
                            queue: stReq.requests
                        }));
                    }
                    ws.send(JSON.stringify({ type: 'request-sent' }));
                }
                break;

            case 'request-action':
                if (!ws.station || ws.role !== 'broadcaster') return;
                var stAct = stations.get(ws.station);
                if (stAct) {
                    var removed = null;
                    stAct.requests = stAct.requests.filter(function(r) {
                        if (r.id === msg.requestId) { removed = r; return false; }
                        return true;
                    });
                    if (msg.accepted && removed) {
                        stAct.listeners.forEach(function(l) {
                            if (l.readyState === 1 && l.nick === removed.nick) {
                                l.send(JSON.stringify({
                                    type: 'request-accepted',
                                    song: removed.song
                                }));
                            }
                        });
                    }
                    ws.send(JSON.stringify({
                        type: 'queue-update',
                        queue: stAct.requests
                    }));
                }
                break;

            case 'block-listener':
                if (!ws.station || ws.role !== 'broadcaster') return;
                var stBlk = stations.get(ws.station);
                if (stBlk) {
                    stBlk.blocked.add(msg.nick.toLowerCase());
                    stBlk.listeners.forEach(function(l) {
                        if (l.nick && l.nick.toLowerCase() === msg.nick.toLowerCase()) {
                            l.blocked = true;
                            l.send(JSON.stringify({ type: 'blocked' }));
                            setTimeout(function() {
                                try { l.close(); } catch(e) {}
                            }, 500);
                        }
                    });
                    sendAll(ws.station, {
                        type: 'system',
                        text: msg.nick + ' atildi'
                    }, ws);
                }
                break;

            case 'broadcast-start':
                if (!ws.station) return;
                var stBs = stations.get(ws.station);
                if (stBs) {
                    stBs.song = msg.song || '';
                    if (msg.schedule) stBs.schedule = msg.schedule;
                    sendAll(ws.station, {
                        type: 'broadcast-start',
                        nick: ws.nick,
                        song: stBs.song,
                        sampleRate: msg.sampleRate || 44100,
                        schedule: stBs.schedule || []
                    }, ws);
                }
                break;

            case 'broadcast-stop':
                if (!ws.station) return;
                var stBp = stations.get(ws.station);
                if (stBp) {
                    stBp.bc = null;
                    stBp.song = '';
                    sendAll(ws.station, { type: 'broadcast-stop' });
                    stBp.listeners.forEach(function(l) {
                        try { l.close(); } catch(e) {}
                    });
                    stBp.listeners.clear();
                    stations.delete(ws.station);
                }
                break;

            case 'song-update':
                if (!ws.station) return;
                var stSu = stations.get(ws.station);
                if (stSu) {
                    stSu.song = msg.song || '';
                    sendAll(ws.station, {
                        type: 'song-update',
                        song: stSu.song
                    }, ws);
                }
                break;

            case 'update-schedule':
                if (!ws.station || ws.role !== 'broadcaster') return;
                var stSch = stations.get(ws.station);
                if (stSch) {
                    stSch.schedule = msg.schedule || [];
                    sendAll(ws.station, {
                        type: 'schedule-update',
                        schedule: stSch.schedule
                    });
                }
                break;

            default:
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
            st.listeners.forEach(function(l) {
                try { l.close(); } catch(e) {}
            });
            st.listeners.clear();
            stations.delete(ws.station);
        } else if (ws.role === 'listener' && !ws.blocked) {
            st.listeners.delete(ws);
            var cnt = st.listeners.size + (st.bc ? 1 : 0);
            if (st.bc && st.bc.readyState === 1) {
                st.bc.send(JSON.stringify({
                    type: 'listener-update',
                    count: cnt,
                    stats: st.stats
                }));
            }
        }
    });

    ws.on('pong', () => { ws.alive = true; });
});

setInterval(() => {
    wss.clients.forEach((ws) => {
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
    st.listeners.forEach(function(l) {
        if (l !== exclude && !l.blocked && l.readyState === 1) {
            l.send(data);
        }
    });
}

server.listen(PORT, () => {
    console.log('Radio Studio v3 (Worklet + PIN) sunucu hazir - port ' + PORT);
});