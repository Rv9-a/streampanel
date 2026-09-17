const express = require('express');
const cors = require('cors');
const axios = require('axios');
const fs = require('fs');
const { spawn } = require('child_process');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const NodeMediaServer = require('node-media-server');

const app = express();
const PORT = 3000;

// --- تشغيل خادم RTMP لاستقبال البث من OBS ---
const nmsConfig = {
  rtmp: {
    port: 1935,
    chunk_size: 60000,
    gop_cache: true,
    ping: 30,
    ping_timeout: 60
  },
  http: {
    port: 8000,
    allow_origin: '*'
  }
};
const nms = new NodeMediaServer(nmsConfig);
nms.run();

app.use(cors());

// محلل multipart/form-data مدمج (بلا حزم إضافية) — بعض تطبيقات Xtream ترسل الـ credentials بهذه الصيغة
function parseMultipart(buffer, boundary) {
    const result = {};
    const sep = Buffer.from(`--${boundary}`);
    let start = buffer.indexOf(sep) + sep.length + 2;
    while (true) {
        const end = buffer.indexOf(sep, start);
        if (end === -1) break;
        const part = buffer.slice(start, end - 2);
        const hEnd = part.indexOf('\r\n\r\n');
        if (hEnd !== -1) {
            const nameMatch = part.slice(0, hEnd).toString().match(/name="([^"]+)"/);
            if (nameMatch) result[nameMatch[1]] = part.slice(hEnd + 4).toString().trim();
        }
        start = end + sep.length + 2;
    }
    return result;
}

app.use((req, res, next) => {
    const ct = req.headers['content-type'] || '';
    if (!ct.includes('multipart/form-data')) return next();
    const boundary = ct.match(/boundary="?([^"\s;]+)"?/);
    if (!boundary) return next();
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => { req.body = parseMultipart(Buffer.concat(chunks), boundary[1]); next(); });
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const db = new sqlite3.Database(path.join(__dirname, 'panel.db'), (err) => {
    if (err) console.error("Database Connection Error:", err.message);
});

const channelTypes = {};
const adminSessions = {};
const ffmpegProcesses = {};
const deviceSessions = {}; // { [username]: Map<deviceKey, { ip, lastActive }> }
const streamIdMap = {}; // { <numeric xtream id>: channelId string }
const categoryIdMap = {}; // { <numeric category id>: group_title }
const DEVICE_SESSION_TIMEOUT = 60000; // 60s بدون أي طلب مقطع = الجهاز انقطع
const channelRetries = {};
const channelAlwaysOn = {};
const channelLastAccess = {};
const MAX_CHANNEL_RETRIES = 5;
const IDLE_TIMEOUT_MS = 120000;
const IDLE_CHECK_MS = 30000;

function getClientKey(req) {
    const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '0.0.0.0';
    const deviceId = req.query.deviceId;
    return (deviceId ? String(deviceId).slice(0, 64) : ip);
}

// يبني خارطة الأرقام من القنوات (لتطبيقات Xtream التي تتطلب stream_id رقمياً)
function rebuildXtreamMaps(cb) {
    db.all(`SELECT rowid, id, group_title FROM channels ORDER BY rowid`, [], (err, rows) => {
        Object.keys(streamIdMap).forEach(k => delete streamIdMap[k]);
        Object.keys(categoryIdMap).forEach(k => delete categoryIdMap[k]);
        (rows || []).forEach(r => {
            streamIdMap[r.rowid] = r.id;
            if (!(r.group_title in categoryIdMap)) {
                categoryIdMap[r.group_title] = Object.keys(categoryIdMap).length + 1;
            }
        });
        if (cb) cb();
    });
}

function pruneDeviceSessions(username) {
    const map = deviceSessions[username];
    if (!map) return;
    const now = Date.now();
    for (const [k, s] of map) {
        if (now - s.lastActive > DEVICE_SESSION_TIMEOUT) map.delete(k);
    }
}

// فرض الحد (طرد الأقدم عند الامتلاء) — يتبع نمط Xtream التقليدي
function registerDeviceSession(username, maxConnections, key, ip) {
    pruneDeviceSessions(username);
    let map = deviceSessions[username];
    if (!map) { map = new Map(); deviceSessions[username] = map; }
    const now = Date.now();
    if (map.has(key)) { map.set(key, { ip, lastActive: now }); return; }
    const limit = parseInt(maxConnections) || 1;
    if (map.size >= limit) {
        let oldestKey = null, oldestT = Infinity;
        for (const [k, s] of map) {
            if (s.lastActive < oldestT) { oldestT = s.lastActive; oldestKey = k; }
        }
        if (oldestKey !== null) {
            map.delete(oldestKey);
            console.log(`[Device:${username}] kicked oldest device (${oldestKey}) to make room`);
        }
    }
    map.set(key, { ip, lastActive: now });
}

// تحديث النشاط: بمفتاح الجهاز و/أو أي جلسة من نفس الـ IP (في حال أجرى المشغل طلب المقاطع بدون deviceId)
function touchDeviceSession(username, key, ip) {
    const map = deviceSessions[username];
    if (!map) return;
    const now = Date.now();
    let touched = false;
    if (key && map.has(key)) { map.set(key, { ip, lastActive: now }); touched = true; }
    for (const [k, s] of map) {
        if (s.ip === ip && k !== key) { map.set(k, { ip, lastActive: now }); touched = true; }
    }
    return touched;
}

function authUser(username, password, cb) {
    db.get(`SELECT * FROM users WHERE username = ? AND password = ? AND status = 1`, [username, password], (err, user) => {
        if (err || !user) return cb(null);
        if (new Date(user.expire_date) < new Date()) return cb(null);
        cb(user);
    });
}

function serverInfoFor(req) {
    const host = req.headers.host || `localhost:${PORT}`;
    const hostName = host.split(':')[0];
    return {
        url: hostName,
        port: String(PORT),
        https_port: String(PORT),
        server_protocol: 'http',
        rtmp_port: '1935',
        timezone: 'Asia/Riyadh',
        timestamp_now: Math.floor(Date.now() / 1000),
        time_now: new Date().toISOString().replace('T', ' ').slice(0, 19)
    };
}

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        max_connections INTEGER DEFAULT 1,
        expire_date TEXT,
        status INTEGER DEFAULT 1
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS channels (
        id TEXT PRIMARY KEY,
        name TEXT,
        url TEXT,
        stream_type INTEGER DEFAULT 0,
        group_title TEXT DEFAULT 'سيرفر 1',
        always_on INTEGER DEFAULT 0
    )`);

    db.run(`ALTER TABLE channels ADD COLUMN always_on INTEGER DEFAULT 0`, (migErr) => {
        if (!migErr) {
            console.log('[DB] migrated: added always_on column');
        } else if (migErr.message && migErr.message.includes('duplicate column')) {
            // العمود موجود أصلاً — قاعدة بيانات أخرى أو جدول جديد يشمل العمود
        } else {
            console.error('[DB] migration check:', migErr.message);
        }

        const stmt = db.prepare(`INSERT OR REPLACE INTO channels (id, name, url, stream_type, group_title, always_on) VALUES (?, ?, ?, ?, ?, ?)`);
        stmt.on('error', (e) => {
            console.error(`[DB] channel insert error:`, e.message);
        });
        defaultChannels.forEach(c => {
            stmt.run(c, (e) => {
                if (e) console.error(`[DB] insert error for ${c[0]}:`, e.message);
            });
        });
        stmt.finalize();

        console.log(`[DB] default channels ready: ${defaultChannels.length} total`);

        db.run(`DELETE FROM channels WHERE id LIKE 'besp%'
                OR id IN ('4k')
                OR (id LIKE 'alwan%' AND id NOT LIKE 'alwan_hd%' AND id NOT LIKE 'alwan_4k%')
                OR id IN ('bein1_4k', 'bein2_4k', 'bein3_4k', 'bein4_4k', 'bein5_4k', 'bein6_4k', 'bein7_4k', 'bein8_4k', 'bein9_4k')`, (cleanErr) => {
            if (cleanErr) console.error('[DB] cleanup error:', cleanErr.message);
        });

        db.all(`SELECT * FROM channels`, [], (err, rows) => {
            if (!err && rows) {
                const alwaysChannels = rows.filter(r => r.always_on);
                if (alwaysChannels.length) console.log(`[DB] starting ${alwaysChannels.length} always-on channels at boot`);
                rows.forEach(ch => {
                    channelTypes[ch.id] = ch.stream_type;
                    channelAlwaysOn[ch.id] = !!ch.always_on;
                    if (ch.always_on) {
                        startChannelProcess(ch.id, ch.url, ch.stream_type, true, ch.group_title);
                    }
                });
                rebuildXtreamMaps();
            }
        });
    });
});

    const SEP_URL = 'dummy://separator';
const groupRV = 'bein rv';
const groupSS = 'bein ss';
const groupSrc = 'bein مصدر خاص';
const groupAlK = 'الكاس alkass';
const groupAlwan = 'alwan sport';

const defaultChannels = [];
const mk = (id, name, url, group, always = 0, streamType = 1) => [id, name, url, streamType, group, always];
const sepCh = (id, label, group) => mk(`sep_${id}`, `════ ${label} ════`, SEP_URL, group);

// ── bein rv (تعمل دائماً + بروكسي) ──
for (let i = 1; i <= 7; i++) {
    defaultChannels.push(mk(`bein${i}`, `beIN Sports ${i} FHD`, `https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN${i}FHD.m3u8`, groupRV, 1, 0));
}
defaultChannels.push(mk('rvtv_event', 'Rvtv (live event)', 'rtmp://127.0.0.1:1935/live/event', groupRV, 1, 0));

// ── bein ss ──
const ssBase = 'http://pro.netmos.ovh:7355/live/EXMOQNS9Y30998CX0/LKHSB87278DOKCPP/';
const ss4k = ['221764', '221765', '221766', '221767', 'https://prime-fast.sytes.net/prime-tv/stream/78.m3u8', '221769', '221770', '221771'];
defaultChannels.push(sepCh('ss_4k', '4K', groupSS));
defaultChannels.push(mk('bein_ss_4k_true', 'bein 4K (true 4k)', `${ssBase}158960.ts`, groupSS));
defaultChannels.push(mk('bein_ss_4k_event', 'bein 4K (only event)', `${ssBase}158961.ts`, groupSS));
defaultChannels.push(sepCh('ss_4k_fhd', '4K (FHD)', groupSS));
ss4k.forEach((u, i) => {
    const url = u.startsWith('http') ? u : `${ssBase}${u}.ts`;
    defaultChannels.push(mk(`bein_ss_4k${i + 1}`, `beinsport 4K ${i + 1} FHD`, url, groupSS));
});
defaultChannels.push(sepCh('ss_misc', 'متنوعة', groupSS));
defaultChannels.push(mk('bein_ss_news', 'bein news', `${ssBase}83618.ts`, groupSS));
defaultChannels.push(mk('bein_ss_global', 'bein global', `${ssBase}231675.ts`, groupSS));
defaultChannels.push(sepCh('ss_sd', 'SD', groupSS));
['102890', '102891', '108484', '108485', '158897', '102895', '108486', '158898', '158899'].forEach((u, i) => {
    defaultChannels.push(mk(`bein_ss_sd${i + 1}`, `beinsport ${i + 1} SD`, `${ssBase}${u}.ts`, groupSS));
});
defaultChannels.push(sepCh('ss_hd', 'HD', groupSS));
['158866', '158889', '158890', '158891', '158892', '158893', '158894', '158895', '158896'].forEach((u, i) => {
    defaultChannels.push(mk(`bein_ss_hd${i + 1}`, `beinsport ${i + 1} HD`, `${ssBase}${u}.ts`, groupSS));
});
defaultChannels.push(sepCh('ss_fhd', 'FHD', groupSS));
['158867', '158900', '158901', '158902', '158903', '158904', '158905', '158906', '158907'].forEach((u, i) => {
    defaultChannels.push(mk(`bein_ss_fhd${i + 1}`, `beinsport ${i + 1} FHD`, `${ssBase}${u}.ts`, groupSS));
});

// ── bein مصدر خاص ──
const pfBase = 'https://prime-fast.sytes.net/prime-tv/stream/';
defaultChannels.push(sepCh('src_4k', '4K', groupSrc));
for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_src4k${i}`, `bein ${i} 4K`, `${pfBase}${73 + i}.m3u8`, groupSrc));
defaultChannels.push(sepCh('src_uhd', 'UHD', groupSrc));
for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srcuhd${i}`, `bein sports ${i} (UHD)`, `http://fackyou-cdn5.cfd/BEIN-${i}/index.m3u8`, groupSrc));
defaultChannels.push(sepCh('src_fhd', 'FHD', groupSrc));
for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srcfhd${i}`, `bein sports ${i} FHD`, `${pfBase}${62 + i}.m3u8`, groupSrc));
defaultChannels.push(sepCh('src_hd', 'HD', groupSrc));
for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srchd${i}`, `bein sports ${i} HD`, `${pfBase}${51 + i}.m3u8`, groupSrc));
defaultChannels.push(sepCh('src_sd', 'SD', groupSrc));
for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srcsd${i}`, `bein sports ${i} SD`, `${pfBase}${24 + i}.m3u8`, groupSrc));
defaultChannels.push(sepCh('src_mob', 'وقت المباريات', groupSrc));
for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srcmob${i}`, `bein sports ${i}`, `http://82.39.115.26:3000/live/${i}.m3u8`, groupSrc));

// ── الكاس alkass ──
const kWords = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight'];
for (let i = 0; i < 8; i++) defaultChannels.push(mk(`alkass${i + 1}`, `الكأس ${i + 1}`, `https://alkass.kianezidi.workers.dev/${kWords[i]}.m3u8`, groupAlK));

// ── alwan sport ──
defaultChannels.push(sepCh('alwan_hd', 'HD', groupAlwan));
['232595', '232596', '232597', '232598', '232599', '232600'].forEach((u, i) => {
    defaultChannels.push(mk(`alwan_hd${i + 1}`, `ALWAN SPORT ${i + 1} HD`, `${ssBase}${u}.ts`, groupAlwan));
});
defaultChannels.push(sepCh('alwan_4k', '4K', groupAlwan));
['232601', '232602', '232603', '232604', '232605', '232606'].forEach((u, i) => {
    defaultChannels.push(mk(`alwan_4k${i + 1}`, `ALWAN SPORT ${i + 1} 4K`, `${ssBase}${u}.ts`, groupAlwan));
});

// ── Rotana l روتانا (محمي — ريفير rotana.net حصرياً) ──
const groupRot = 'Rotana l روتانا';
const rotBase = 'https://rotana.hibridcdn.net/rotananet/';
const rotChannels = [
    ['rotana_cinema', 'Rotana Cinema', `cinema_net-7Y83PP5adWixDF93/playlist.m3u8`],
    ['rotana_masr', 'Rotana Cinema Masr', `cinemamasr_net-7Y83PP5adWixDF93/playlist.m3u8`],
    ['rotana_comedy', 'Rotana Comedy', `comedy_net-7Y83PP5adWixDF93/playlist.m3u8`],
    ['rotana_classical', 'Rotana Classical', `classical_net-7Y83PP5adWixDF93/playlist.m3u8`],
    ['rotana_drama', 'Rotana Drama', `drama_net-7Y83PP5adWixDF93/playlist.m3u8`],
    ['rotana_khaleejiya', 'Rotana Khaleejiya', `khaleejiya_net-7Y83PP5adWixDF93/playlist.m3u8`],
    ['rotana_lbc', 'Rotana LBC', `lbc_net-7Y83PP5adWixDF93/playlist.m3u8`],
    ['rotana_risala', 'Rotana Risala', `risala_net-7Y83PP5adWixDF93/playlist.m3u8`]
];
rotChannels.forEach(c => defaultChannels.push(mk(c[0], c[1], `${rotBase}${c[2]}`, groupRot, 0, 0)));

app.use('/hls', express.static(path.join(__dirname, 'dummy_sep')));

const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://www.maziikaaaaaa.shop/',
    'Origin': 'https://www.maziikaaaaaa.shop'
};

const ROTANA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Referer': 'https://rotana.net/',
    'Origin': 'https://rotana.net/'
};

function getHeadersForUrl(url) {
    if (url && url.includes('rotana.hibridcdn.net')) return ROTANA_HEADERS;
    return HEADERS;
}

app.get('/proxy-seg', async (req, res) => {
    let targetUrl = req.query.url;
    if (!targetUrl) return res.status(400).send('Missing URL');

    const isRotana = targetUrl.includes('rotana.hibridcdn.net');

    try {
        const response = await axios.get(targetUrl, {
            responseType: 'arraybuffer',
            headers: getHeadersForUrl(targetUrl),
            timeout: 15000
        });

        let buffer = Buffer.from(response.data);

        if (targetUrl.includes('.m3u8') || targetUrl.includes('.json')) {
            if (isRotana) console.log(`[Proxy-Rotana] m3u8 OK ${buffer.length}b from ${targetUrl.slice(-60)}`);
            res.setHeader('Content-Type', 'application/x-mpegURL');
            let text = buffer.toString('utf8');
            const baseUri = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
            let modified = text.split('\n').map(line => {
                let trimmed = line.trim();
                if (!trimmed || trimmed.startsWith('#')) return line;
                if (trimmed.startsWith('http')) {
                    return `http://127.0.0.1:${PORT}/proxy-seg?url=${encodeURIComponent(trimmed)}`;
                }
                const resolved = new URL(trimmed, baseUri).href;
                return `http://127.0.0.1:${PORT}/proxy-seg?url=${encodeURIComponent(resolved)}`;
            }).join('\n');
            return res.send(modified);
        }

        let cleanBuffer = buffer;
        if (buffer.length > 1280 && buffer[1280] === 0x47) {
            cleanBuffer = buffer.slice(1280);
        } else {
            let pos = 1280;
            for (let i = 0; i < Math.min(buffer.length, 3000); i++) {
                if (buffer[i] === 0x47 && (i + 188 < buffer.length) && buffer[i + 188] === 0x47) {
                    pos = i;
                    break;
                }
            }
            cleanBuffer = buffer.slice(pos);
        }

        res.setHeader('Content-Type', 'video/mp2t');
        res.setHeader('Content-Length', cleanBuffer.length);
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.send(cleanBuffer);

    } catch (err) {
        if (isRotana) console.log(`[Proxy-Rotana] ERROR ${err.code || err.message} on ${targetUrl.slice(-80)}`);
        res.status(500).send("Proxy Error");
    }
});

app.get('/debug/rotana', async (req, res) => {
    const masterUrl = 'https://rotana.hibridcdn.net/rotananet/cinema_net-7Y83PP5adWixDF93/playlist.m3u8';
    const result = { master: null, variant: null, chunk: null, error: null };
    try {
        const m = await axios.get(masterUrl, { responseType: 'arraybuffer', headers: ROTANA_HEADERS, timeout: 10000 });
        const mText = Buffer.from(m.data).toString('utf8');
        result.master = { status: m.status, size: m.data.byteLength, firstLine: mText.split('\n').find(l => l.includes('chunks')) || 'none' };

        const variantUrl = mText.split('\n').find(l => l.includes('chunks.m3u8') && !l.startsWith('#'));
        if (!variantUrl) throw new Error('no variant found in master');
        const vFull = new URL(variantUrl, masterUrl.substring(0, masterUrl.lastIndexOf('/') + 1)).href;
        const v = await axios.get(vFull, { responseType: 'arraybuffer', headers: ROTANA_HEADERS, timeout: 10000 });
        const vText = Buffer.from(v.data).toString('utf8');
        result.variant = { status: v.status, size: v.data.byteLength, segmentCount: vText.split('\n').filter(l => l.includes('.ts')).length };

        const chunkUrl = vText.split('\n').find(l => l.includes('.ts') && !l.startsWith('#'));
        if (!chunkUrl) throw new Error('no chunk found in variant');
        const cFull = new URL(chunkUrl, vFull.substring(0, vFull.lastIndexOf('/') + 1)).href;
        const c = await axios.get(cFull, { responseType: 'arraybuffer', headers: ROTANA_HEADERS, timeout: 15000 });
        result.chunk = { status: c.status, size: c.data.byteLength, startsWith0x47: c.data[0] === 0x47 };

        result.ok = true;
    } catch (err) {
        result.error = err.code || err.message;
    }
    res.json(result);
});

function startChannelProcess(id, url, streamType = 0, alwaysOn = false, group = '') {
    if (ffmpegProcesses[id]) return;

    channelAlwaysOn[id] = alwaysOn;
    if (!alwaysOn) console.log(`[On-Demand start - ${id}]: starting ffmpeg`);

    const channelDir = path.join(__dirname, id);
    if (!fs.existsSync(channelDir)) {
        fs.mkdirSync(channelDir, { recursive: true });
    }

    const isRtmp = url.startsWith('rtmp://');
    const isDirect = parseInt(streamType) === 1;
    const isBeinRv = group === groupRV && !isRtmp; // buffered mode: bein rv only, NOT rvtv_event

    let inputSource = url;
    if (!isRtmp && !isDirect) {
        inputSource = `http://127.0.0.1:${PORT}/proxy-seg?url=${encodeURIComponent(url)}`;
    }

    const outputPath = path.join(channelDir, 'index.m3u8');

    let ffmpegArgs = ['-y', '-loglevel', 'error'];

    if (isDirect && !isRtmp) {
        ffmpegArgs.push('-user_agent', HEADERS['User-Agent']);
    } else if (!isRtmp) {
        ffmpegArgs.push(
            '-user_agent', HEADERS['User-Agent'],
            '-headers', `Referer: ${HEADERS['Referer']}\r\nOrigin: ${HEADERS['Origin']}\r\n`
        );
    }

    let hlsTime = isRtmp ? '4' : '2';
    let hlsListSize = isRtmp ? '10' : '6';
    let rwTimeout = '30000000'; // 30 ثانية للجميع

    if (isBeinRv) {
        hlsTime = '4';
        hlsListSize = '10';
        rwTimeout = '60000000'; // 60 ثانية لـ bein rv
    }

    if (!isRtmp) {
        ffmpegArgs.push('-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '10');
    }

    ffmpegArgs.push(
        '-rw_timeout', rwTimeout,
        '-analyzeduration', '10000000',
        '-probesize', '10000000',
        '-i', inputSource,
        '-c', 'copy',
        '-f', 'hls', 
        '-hls_time', hlsTime, 
        '-hls_list_size', hlsListSize, 
        '-hls_flags', 'delete_segments+append_list+omit_endlist',
        outputPath
    );

    let proc;
    try {
        proc = spawn('ffmpeg', ffmpegArgs);
    } catch (err) {
        console.error(`[spawn error - ${id}]: ${err.message}`);
        scheduleChannelRetry(id);
        return;
    }

    ffmpegProcesses[id] = proc;

    proc.stderr.on('data', (data) => {
        const msg = data.toString().trim();
        if (msg) console.log(`[FFmpeg ${id}]: ${msg}`);
    });

    proc.on('error', (err) => {
        console.error(`[FFmpeg error - ${id}]: ${err.message}`);
        delete ffmpegProcesses[id];
        scheduleChannelRetry(id, isRtmp);
    });

    proc.on('close', (code) => {
        delete ffmpegProcesses[id];
        if (code !== 0) {
            console.error(`[FFmpeg exit - ${id}]: code ${code}`);
            scheduleChannelRetry(id, isRtmp);
        } else {
            console.log(`[FFmpeg exit - ${id}]: normal exit`);
        }
    });
}

function scheduleChannelRetry(id, isRtmp = false) {
    if (ffmpegProcesses[id]) return;

    const alwaysOn = !!channelAlwaysOn[id];

    if (!alwaysOn && Date.now() - (channelLastAccess[id] || 0) > 15000) {
        channelRetries[id] = 0;
        console.log(`[FFmpeg idle - ${id}]: on-demand with no active viewers, staying stopped`);
        return;
    }

    const attempt = channelRetries[id] || 0;

    if (isRtmp) {
        channelRetries[id] = attempt + 1;
        const delay = 10000;
        console.log(`[FFmpeg retry - ${id}]: RTMP waiting for source, retry in ${delay / 1000}s`);
        setTimeout(() => {
            db.get(`SELECT * FROM channels WHERE id = ?`, [id], (err, ch) => {
                if (ch && !ffmpegProcesses[id]) startChannelProcess(ch.id, ch.url, ch.stream_type, !!channelAlwaysOn[id], ch.group_title);
            });
        }, delay);
        return;
    }

    const maxRetries = alwaysOn ? MAX_CHANNEL_RETRIES : 3;
    if (attempt >= maxRetries) {
        console.error(`[FFmpeg stop - ${id}]: max retries reached, stopping`);
        channelRetries[id] = 0;
        return;
    }
    channelRetries[id] = attempt + 1;
    const delays = alwaysOn ? [3000, 15000, 30000, 60000, 120000] : [3000, 6000, 12000];
    const delay = delays[Math.min(attempt, delays.length - 1)];
    console.log(`[FFmpeg retry - ${id}]: attempt ${attempt + 1}/${maxRetries} in ${delay / 1000}s`);
    setTimeout(() => {
        db.get(`SELECT * FROM channels WHERE id = ?`, [id], (err, ch) => {
            if (ch && !ffmpegProcesses[id]) {
                channelRetries[id] = 0;
                startChannelProcess(ch.id, ch.url, ch.stream_type, !!channelAlwaysOn[ch.id], ch.group_title);
            }
        });
    }, delay);
}

function cleanChannelDir(id) {
    const dir = path.join(__dirname, id);
    if (!fs.existsSync(dir)) return;
    try {
        fs.readdirSync(dir).forEach(f => {
            if (f.endsWith('.ts') || f === 'index.m3u8') {
                fs.unlinkSync(path.join(dir, f));
            }
        });
    } catch (err) {
        console.error(`[cleanDir - ${id}]: ${err.message}`);
    }
}

const DUMMY_DIR = path.join(__dirname, 'dummy_sep');
function ensureDummyAsset() {
    const out = path.join(DUMMY_DIR, 'index.m3u8');
    if (fs.existsSync(out)) return;
    if (!fs.existsSync(DUMMY_DIR)) fs.mkdirSync(DUMMY_DIR, { recursive: true });
    const args = ['-y', '-loglevel', 'error',
        '-f', 'lavfi', '-i', 'color=c=black:s=640x360:r=10',
        '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
        '-t', '60',
        '-c:v', 'libx264', '-preset', 'veryfast', '-pix_fmt', 'yuv420p',
        '-c:a', 'aac', '-ac', '2', '-ar', '44100',
        '-f', 'hls', '-hls_time', '2', '-hls_list_size', '30',
        '-hls_flags', 'delete_segments+append_list',
        out];
    console.log('[Dummy] generating separator video asset (one-time)...');
    const proc = spawn('ffmpeg', args);
    proc.on('error', (e) => console.error('[Dummy] spawn error:', e.message));
    proc.on('close', (code) => console.log(`[Dummy] separator asset ready (exit ${code})`));
    proc.stderr.on('data', () => {});
}

function checkIdleChannels() {
    const now = Date.now();
    for (const id of Object.keys(ffmpegProcesses)) {
        if (channelAlwaysOn[id]) continue;
        const last = channelLastAccess[id] || 0;
        if (now - last > IDLE_TIMEOUT_MS) {
            console.log(`[On-Demand stop - ${id}]: idle ${Math.round((now - last) / 1000)}s, stopping ffmpeg`);
            try { ffmpegProcesses[id].kill('SIGKILL'); } catch (e) { /* already gone */ }
            delete ffmpegProcesses[id];
            delete channelRetries[id];
            cleanChannelDir(id);
        }
    }
}
setInterval(checkIdleChannels, IDLE_CHECK_MS);

setInterval(() => {
    for (const username of Object.keys(deviceSessions)) {
        pruneDeviceSessions(username);
        if (deviceSessions[username].size === 0) delete deviceSessions[username];
    }
}, 15000);

const serveChannelPlaylist = (req, res) => {
    const username = req.params.username;
    const password = req.params.password;
    const rawChannelId = req.params.channelId;
    const channelId = streamIdMap[rawChannelId] || rawChannelId;

    authUser(username, password, (user) => {
        if (!user) return res.status(403).send('Unauthorized');

        const deviceKey = getClientKey(req);
        const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '0.0.0.0';
        registerDeviceSession(username, user.max_connections, deviceKey, ip);

        db.get(`SELECT * FROM channels WHERE id = ?`, [channelId], (err, channel) => {
            if (!channel) return res.status(404).send('Not Found');

            channelLastAccess[channelId] = Date.now();

            if (channel.url && channel.url.startsWith('dummy://')) {
                return res.redirect(`/live/${username}/${password}/dummy_sep/index.m3u8`);
            }

            const alwaysOn = !!channelAlwaysOn[channelId];

            if (!ffmpegProcesses[channelId]) {
                if (!alwaysOn) cleanChannelDir(channelId);
                startChannelProcess(channelId, channel.url, channel.stream_type, alwaysOn, channel.group_title);
            }

            channelLastAccess[channelId] = Date.now();
            // رد فوري بلا انتظار: التطبيق يعتبر الاتصال نجح فوراً،
            // والـ placeholder playlist تغطّي مدة بناء البث حتى يجيء الـ index.m3u8 الحقيقي
            return res.redirect(`/live/${username}/${password}/${channelId}/index.m3u8`);
        });
    });
};

// يقبل الصيغتين m3u8 و ts — بعض التطبيقات (مثل Next+) تطلب القناة بصيغة .ts
app.get('/live/:username/:password/:channelId.m3u8', serveChannelPlaylist);
app.get('/live/:username/:password/:channelId.ts', serveChannelPlaylist);

// خدمة ملفات البث (playlist + المقاطع) تحت مسار مصادق عليه — كل مقطع يمر بالتحقق ويجدّد الجلسة
// (regex path لتوافق Express 4 و 5)
app.get(/^\/live\/([^/]+)\/([^/]+)\/([^/]+)\/(.+)$/, (req, res) => {
    const username = req.params[0];
    const password = req.params[1];
    const channelId = req.params[2];
    const file = req.params[3];

    authUser(username, password, (user) => {
        if (!user) return res.status(403).send('Unauthorized');

        const deviceKey = getClientKey(req);
        const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '0.0.0.0';
        touchDeviceSession(username, deviceKey, ip);

        const channelDir = path.join(__dirname, channelId);
        const fullPath = path.normalize(path.join(channelDir, file));
        if (!fullPath.startsWith(channelDir + path.sep)) {
            return res.status(403).send('Forbidden');
        }

        const ext = path.extname(file).toLowerCase();
        const isIndex = file === 'index.m3u8';

        // placeholder: إذا index.m3u8 غير موجود لكن القناة معروفة → نعيد playlist فارغ
        // بدلاً من 404 — بالتالي ينتظر الـ app (HLS retry) وتظهر القنوات فور جاهزيتها
        if (isIndex && !fs.existsSync(fullPath) && channelTypes[channelId] !== undefined) {
            res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
            res.setHeader('Access-Control-Allow-Origin', '*');
            return res.send('#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:0\n');
        }

        if (!fs.existsSync(fullPath)) return res.status(404).send('Not Found');

        if (ext === '.m3u8') {
            res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        } else {
            res.setHeader('Content-Type', 'video/mp2t');
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        }
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.sendFile(fullPath);
    });
});

app.get('/playlist/:username/:password/get.m3u', (req, res) => {
    const { username, password } = req.params;
    const host = req.headers.host;

    db.get(`SELECT * FROM users WHERE username = ? AND password = ? AND status = 1`, [username, password], (err, user) => {
        if (err || !user) return res.status(403).send('Unauthorized');
        const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '0.0.0.0';
        registerDeviceSession(user.username, user.max_connections, getClientKey(req), ip);

        db.all(`SELECT * FROM channels ORDER BY rowid`, [], (err, channels) => {
            let m3uContent = `#EXTM3U\n`;
            channels.forEach(ch => {
                const groupName = ch.group_title || 'سيرفر 1';
                m3uContent += `#EXTINF:-1 tvg-id="${ch.id}" tvg-name="${ch.name}" group-title="${groupName}",${ch.name}\n`;
                m3uContent += `http://${host}/live/${username}/${password}/${ch.id}.m3u8\n`;
            });

            res.setHeader('Content-Type', 'audio/x-mpegurl');
            res.send(m3uContent);
        });
    });
});

// ─── Xtream Codes API (لبرامج مثل 1stream / IPTV Smarters / OTT Navigator) ───
function xtreamCreds(req) {
    const q = req.query || {};
    const b = req.body || {};
    if (q.username && q.password) return { username: q.username, password: q.password };
    if (b.username && b.password) return { username: b.username, password: b.password };
    const auth = req.headers.authorization || '';
    const m = /Basic\s+([A-Za-z0-9+/=]+)/i.exec(auth);
    if (m) {
        try {
            const dec = Buffer.from(m[1], 'base64').toString('utf8');
            const i = dec.indexOf(':');
            if (i > 0) return { username: dec.slice(0, i), password: dec.slice(i + 1) };
        } catch (e) {}
    }
    return null;
}

app.all(['/player_api.php', '/panel_api.php'], (req, res) => {
    const creds = xtreamCreds(req);
    const bodyAction = (req.body || {}).action;
    const action = req.query.action || bodyAction;
    console.log(`[Xtream] ${req.method} ${req.originalUrl} action=${action || 'login'} u=${creds ? creds.username : 'none'}`);
    if (!creds) {
        return res.json({ user_info: null });
    }
    const { username, password } = creds;

    authUser(username, password, (user) => {
        if (!user) {
            return res.json({
                user_info: { auth: 0, status: 'Disabled', message: 'Invalid credentials or expired' },
                server_info: serverInfoFor(req)
            });
        }

        const activeNow = (() => {
            pruneDeviceSessions(user.username);
            return deviceSessions[user.username] ? deviceSessions[user.username].size : 0;
        })();

        const userInfo = {
            username: user.username,
            password: user.password,
            message: '',
            auth: 1,
            status: 'Active',
            exp_date: Math.floor(new Date(user.expire_date).getTime() / 1000),
            is_trial: '0',
            active_cons: activeNow,
            created_at: '0',
            max_connections: String(user.max_connections || 1),
            allowed_output_formats: ['m3u8', 'ts']
        };

        const baseResp = { user_info: userInfo, server_info: serverInfoFor(req) };

        if (!action) {
            const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '0.0.0.0';
            registerDeviceSession(user.username, user.max_connections, getClientKey(req), ip);
            userInfo.active_cons = deviceSessions[user.username] ? deviceSessions[user.username].size : 0;
            return res.json(baseResp);
        }

        if (action === 'get_live_categories') {
            db.all(`SELECT group_title, MIN(rowid) AS r FROM channels GROUP BY group_title ORDER BY r`, [], (err, rows) => {
                const cats = (rows || []).map((g) => ({
                    category_id: categoryIdMap[g.group_title] || (g.group_title || 'سيرفر 1'),
                    category_name: g.group_title || 'سيرفر 1',
                    parent_id: 0
                }));
                return res.json(cats);
            });
            return;
        }

        if (action === 'get_live_streams') {
            db.all(`SELECT rowid, * FROM channels ORDER BY rowid`, [], (err, channels) => {
                const streams = (channels || []).map((ch, i) => ({
                    num: i + 1,
                    name: ch.name,
                    stream_type: 'live',
                    stream_id: ch.rowid,
                    stream_icon: '',
                    epg_channel_id: '',
                    added: '',
                    category_id: categoryIdMap[ch.group_title] || (ch.group_title || 'سيرفر 1'),
                    custom_sid: '',
                    tv_archive: 0,
                    direct_source: '',
                    tv_archive_duration: 0
                }));
                console.log(`[Xtream] get_live_streams => ${streams.length} channels | categories=${Object.keys(categoryIdMap).length}`);
                return res.json(streams);
            });
            return;
        }

        // لا يوجد VOD / Series / EPG حالياً — مصفوفات فارغة كي لا تتعطل التطبيقات
        if (['get_vod_categories', 'get_vod_streams', 'get_series_categories', 'get_series',
                'get_short_epg', 'get_simple_data_table'].includes(action)) {
            return res.json([]);
        }

        return res.json([]);
    });
});

// endpoint قديم لكلاسيك Xtream — يعيد قائمة M3U (بعض التطبيقات تعتمد عليه حصراً)
app.all('/get.php', (req, res) => {
    const creds = xtreamCreds(req);
    const b = req.body || {};
    if (!creds) { console.log('[get.php] no creds'); return res.status(403).send('Access denied'); }
    authUser(creds.username, creds.password, (user) => {
        if (!user) { console.log('[get.php] auth fail', creds.username); return res.status(403).send('Access denied'); }
        console.log('[get.php] OK', creds.username, 'output=', req.query.output || b.output);
        const type = req.query.type || b.type || 'm3u_plus';
        const ext = String(req.query.output || b.output) === 'ts' ? 'ts' : 'm3u8';
        if (['m3u_plus', 'live', 'live_plus'].includes(String(type))) {
            db.all(`SELECT * FROM channels ORDER BY rowid`, [], (err, channels) => {
                let out = `#EXTM3U\n`;
                (channels || []).forEach((ch) => {
                    const g = (ch.group_title || 'سيرفر 1').replace(/,/g, '،');
                    const n = String(ch.name).replace(/,/g, '،');
                    out += `#EXTINF:-1 tvg-id="${ch.id}" tvg-name="${n}" group-title="${g}",${n}\n`;
                    out += `http://${req.headers.host}/live/${encodeURIComponent(user.username)}/${encodeURIComponent(user.password)}/${ch.id}.${ext}\n`;
                });
                res.setHeader('Content-Type', 'audio/x-mpegurl; charset=utf-8');
                res.send(out);
            });
        } else {
            res.setHeader('Content-Type', 'audio/x-mpegurl; charset=utf-8');
            res.send(`#EXTM3U\n`);
        }
    });
});

// EPG — لا يوجد حالياً، نعيد XMLTV فارغاً كي لا يتعطل التطبيق
app.all('/xmltv.php', (req, res) => {
    const creds = xtreamCreds(req);
    if (!creds) return res.status(403).send('Access denied');
    authUser(creds.username, creds.password, (user) => {
        if (!user) return res.status(403).send('Access denied');
        res.setHeader('Content-Type', 'application/xml; charset=utf-8');
        res.send(`<?xml version="1.0" encoding="UTF-8"?>\n<tv generator-info-name="stream-panel"></tv>`);
    });
});

app.post('/api/login', (req, res) => {
    if (req.body.password === 'admin') {
        const token = Math.random().toString(36).substring(2);
        adminSessions[token] = true;
        return res.json({ success: true, token });
    }
    res.status(401).json({ success: false, error: 'كلمة المرور غير صحيحة' });
});

const checkAdmin = (req, res, next) => {
    const token = req.headers['x-admin-token'] || req.query.token;
    if (token && adminSessions[token]) return next();
    res.status(401).json({ error: 'Unauthorized' });
};

app.get('/api/channels', checkAdmin, (req, res) => {
    db.all(`SELECT * FROM channels`, [], (err, rows) => {
        res.json(rows.map(r => ({ ...r, running: !!ffmpegProcesses[r.id] })));
    });
});

app.post('/api/channels/add', checkAdmin, (req, res) => {
    const { id, name, url, stream_type, group_title } = req.body;
    const sType = parseInt(stream_type) || 0;
    const alwaysOn = req.body.always_on ? 1 : 0;
    
    db.run(`INSERT OR REPLACE INTO channels (id, name, url, stream_type, group_title, always_on) VALUES (?, ?, ?, ?, ?, ?)`, 
        [id, name, url, sType, group_title || 'سيرفر 1', alwaysOn], 
        () => {
            channelTypes[id] = sType;
            channelAlwaysOn[id] = !!alwaysOn;
            if (alwaysOn) startChannelProcess(id, url, sType, true, group_title || 'سيرفر 1');
            rebuildXtreamMaps(() => res.json({ success: true }));
        });
});

app.post('/api/channels/delete', checkAdmin, (req, res) => {
    const { id } = req.body;
    if (ffmpegProcesses[id]) {
        try { ffmpegProcesses[id].kill('SIGKILL'); } catch (e) {}
        delete ffmpegProcesses[id];
    }
    delete channelTypes[id];
    delete channelAlwaysOn[id];
    delete channelLastAccess[id];
    delete channelRetries[id];
    db.run(`DELETE FROM channels WHERE id = ?`, [id], () => {
        rebuildXtreamMaps(() => res.json({ success: true }));
    });
});

app.get('/api/users', checkAdmin, (req, res) => {
    db.all(`SELECT * FROM users`, [], (err, rows) => {
        (rows || []).forEach(u => {
            pruneDeviceSessions(u.username);
            u.connected = deviceSessions[u.username] ? deviceSessions[u.username].size : 0;
        });
        res.json(rows);
    });
});

app.get('/api/stats', checkAdmin, (req, res) => {
    db.all(`SELECT * FROM channels`, [], (err, channels) => {
        db.all(`SELECT * FROM users`, [], (e2, users) => {
            let connectedDevices = 0;
            (users || []).forEach(u => {
                pruneDeviceSessions(u.username);
                connectedDevices += deviceSessions[u.username] ? deviceSessions[u.username].size : 0;
            });
            const groups = {};
            (channels || []).forEach(c => {
                const g = c.group_title || 'سيرفر 1';
                groups[g] = (groups[g] || 0) + 1;
            });
            res.json({
                totalChannels: (channels || []).length,
                runningChannels: (channels || []).filter(c => !!ffmpegProcesses[c.id]).length,
                totalUsers: (users || []).length,
                connectedDevices,
                groups: Object.entries(groups).map(([name, count]) => ({ name, count }))
            });
        });
    });
});

app.post('/api/users/add', checkAdmin, (req, res) => {
    const { username, password, max_connections, expire_date } = req.body;
    db.run(`INSERT INTO users (username, password, max_connections, expire_date) VALUES (?, ?, ?, ?)`,
        [username, password, max_connections || 1, expire_date],
        () => res.json({ success: true })
    );
});

app.post('/api/users/delete', checkAdmin, (req, res) => {
    db.get(`SELECT username FROM users WHERE id = ?`, [req.body.id], (err, u) => {
        if (u && deviceSessions[u.username]) delete deviceSessions[u.username];
        db.run(`DELETE FROM users WHERE id = ?`, [req.body.id], () => res.json({ success: true }));
    });
});

app.post('/api/users/kick', checkAdmin, (req, res) => {
    db.get(`SELECT username FROM users WHERE id = ?`, [req.body.id], (err, u) => {
        if (u && deviceSessions[u.username]) delete deviceSessions[u.username];
        res.json({ success: true });
    });
});

const adminHtml = (() => {
    try {
        return fs.readFileSync(path.join(__dirname, 'admin.html'), 'utf8');
    } catch (e) {
        console.error('[admin] admin.html not found:', e.message);
        return '<!DOCTYPE html><html dir="rtl"><body style="font-family:sans-serif;background:#0b1020;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh">لوحة التحكم غير متاحة (admin.html مفقود)</body></html>';
    }
})();

app.get('/admin', (req, res) => {
    res.type('html').send(adminHtml);
});

process.on('uncaughtException', (err) => {
    console.error('UNCAUGHT EXCEPTION (panel continues):', err.message);
});

process.on('unhandledRejection', (reason) => {
    console.error('UNHANDLED REJECTION (panel continues):', reason);
});

app.listen(PORT, () => {
    console.log(`IPTV Panel running on port ${PORT}`);
    ensureDummyAsset();
});
app.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        console.error(`Port ${PORT} already in use — restarting manually required`);
    } else {
        console.error('Panel server error:', err.message);
    }
});